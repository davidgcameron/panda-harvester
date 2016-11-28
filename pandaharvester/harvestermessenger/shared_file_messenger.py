import json
import os
import os.path
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.file_spec import FileSpec
from pandaharvester.harvestercore.event_spec import EventSpec
from pandaharvester.harvestercore.plugin_base import PluginBase

# logger
_logger = core_utils.setup_logger()

# json for worker attributes
jsonAttrsFileName = 'worker_attributes.json'

# json for outputs
jsonOutputsFileName = 'event_status.dump.json'

# xml for outputs
xmlOutputsBaseFileName = '_event_status.dump'

# json for job request
jsonJobRequestFileName = 'worker_requestjob.json'

# json for job spec
jsonJobSpecFileName = 'HPCJobs.json'

# json for event request
jsonEventsRequestFileName = 'worker_requestevents.json'

# json to feed events
jsonEventsFeedFileName = 'JobsEventRanges.json'

# json to update events
jsonEventsUpdateFileName = 'worker_updateevents.json'

# PFC for input files
xmlPoolCatalogFileName = 'PoolFileCatalog_H.xml'

# suffix to read json
suffixReadJson = '.read'


# messenger with shared file system
class SharedFileMessenger(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # update job attributes with workers
    def update_job_attributes_with_workers(self, map_type, jobspec_list, workspec_list, files_to_stage_out,
                                           events_to_update):
        if map_type == WorkSpec.MT_OneToOne:
            jobSpec = jobspec_list[0]
            workSpec = workspec_list[0]
            tmpLog = core_utils.make_logger(_logger, 'PandaID={0} workerID={1}'.format(jobSpec.PandaID,
                                                                                       workSpec.workerID))
            jobSpec.set_attributes(workSpec.workAttributes)
            # set start and end times
            if workSpec.status in [WorkSpec.ST_running]:
                jobSpec.set_start_time()
            elif workSpec.status in [WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled]:
                jobSpec.set_end_time()
            # add files
            if jobSpec.PandaID in files_to_stage_out:
                for lfn, fileAtters in files_to_stage_out[jobSpec.PandaID].iteritems():
                    fileSpec = FileSpec()
                    fileSpec.lfn = lfn
                    fileSpec.PandaID = jobSpec.PandaID
                    fileSpec.taskID = jobSpec.taskID
                    fileSpec.path = fileAtters['path']
                    fileSpec.fsize = fileAtters['fsize']
                    fileSpec.fileType = fileAtters['type']
                    fileSpec.fileAttributes = fileAtters
                    if 'isZip' in fileAtters:
                        fileSpec.isZip = fileAtters['isZip']
                    if 'eventRangeID' in fileAtters:
                        fileSpec.eventRangeID = fileAtters['eventRangeID']
                    jobSpec.add_out_file(fileSpec)
            # add events
            if jobSpec.PandaID in events_to_update:
                for data in events_to_update[jobSpec.PandaID]:
                    eventSpec = EventSpec()
                    eventSpec.from_data(data)
                    jobSpec.add_event(eventSpec, None)
            jobSpec.status, jobSpec.subStatus = workSpec.convert_to_job_status()
            tmpLog.debug('new jobStatus={0} subStatus={1}'.format(jobSpec.status, jobSpec.subStatus))
        elif map_type == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        return True

    # get attributes of a worker which should be propagated to job(s).
    #  * the worker needs to put a json under the access point
    def get_work_attributes(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retDict = {}
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the accesspoint
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonAttrsFileName)
            tmpLog.debug('looking for attributes file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
                return {}
            try:
                with open(jsonFilePath) as jsonFile:
                    retDict = json.load(jsonFile)
            except:
                tmpLog.debug('failed to load json')
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under accesspoint/${PandaID}
            # TOBEFIXED
            pass
        return retDict

    # get files to stage-out.
    #  * the worker needs to put a json under the access point
    def get_files_to_stage_out(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        fileDict = dict()
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # look for the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonOutputsFileName)
            readJsonPath = jsonFilePath + suffixReadJson
            # first look for json.read which is not yet acknowledged
            tmpLog.debug('looking for output file {0}'.format(readJsonPath))
            if os.path.exists(readJsonPath):
                pass
            else:
                tmpLog.debug('looking for event update file {0}'.format(jsonFilePath))
                if not os.path.exists(jsonFilePath):
                    # not found
                    tmpLog.debug('not found')
                    return {}
                try:
                    # rename to prevent from being overwritten
                    os.rename(jsonFilePath, readJsonPath)
                except:
                    tmpLog.error('failed to rename json')
                    return {}
            # load json
            try:
                with open(readJsonPath) as jsonFile:
                    loadDict = json.load(jsonFile)
            except:
                tmpLog.debug('failed to load json')
                return {}
            # collect files and events
            eventsList = dict()
            for tmpPandaID, tmpEventMap in loadDict.iteritems():
                tmpPandaID = long(tmpPandaID)
                for tmpEventRangeID, tmpEventInfo in tmpEventMap.iteritems():
                    tmpFileDict = dict()
                    pfn = tmpEventInfo['path']
                    lfn = os.path.basename(pfn)
                    tmpFileDict['path'] = pfn
                    tmpFileDict['fsize'] = os.stat(pfn).st_size
                    tmpFileDict['type'] = 'es_output'
                    tmpFileDict['eventRangeID'] = tmpEventRangeID
                    if tmpPandaID not in fileDict:
                        fileDict[tmpPandaID] = dict()
                    fileDict[tmpPandaID][lfn] = tmpFileDict
                    if tmpPandaID not in eventsList:
                        eventsList[tmpPandaID] = list()
                    eventsList[tmpPandaID].append({'eventRangeID': tmpEventRangeID,
                                                   'eventStatus': tmpEventInfo['eventStatus']})
            # dump events
            if eventsList != []:
                curName = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
                newName = curName + '.new'
                f = open(newName, 'w')
                json.dump(eventsList, f)
                f.close()
                os.rename(newName, curName)
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # look for json files under access_point/${PandaID}
            # TOBEFIXED
            pass
        tmpLog.debug('got {0}'.format(str(fileDict)))
        return fileDict

    # check if job is requested.
    # * the worker needs to put a json under the access point
    def job_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
        tmpLog.debug('looking for job request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return False
        tmpLog.debug('found')
        return True

    # feed jobs
    # * worker_jobspec.json is put under the access point
    def feed_jobs(self, workspec, jobspec_list):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retVal = True
        # get PFC
        pfc = core_utils.make_pool_file_catalog(jobspec_list)
        if workspec.mapType == WorkSpec.MT_OneToOne:
            jobSpec = jobspec_list[0]
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobSpecFileName)
            xmlFilePath = os.path.join(workspec.get_access_point(), xmlPoolCatalogFileName)
            tmpLog.debug('feeding jobs to {0}'.format(jsonFilePath))
            try:
                # put job spec json
                with open(jsonFilePath, 'w') as jsonFile:
                    json.dump({jobSpec.PandaID: jobSpec.jobParams}, jsonFile)
                # put PFC.xml
                with open(xmlFilePath, 'w') as pfcFile:
                    pfcFile.write(pfc)
                # make symlink
                inFiles = jobSpec.get_input_file_attributes()
                for inLFN, inFile in inFiles.iteritems():
                    dstPath = os.path.join(workspec.get_access_point(), inLFN)
                    os.symlink(inFile['path'], dstPath)
            except:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonJobRequestFileName)
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return retVal

    # request events.
    # * the worker needs to put a json under the access point
    def events_requested(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
        tmpLog.debug('looking for event request file {0}'.format(jsonFilePath))
        if not os.path.exists(jsonFilePath):
            # not found
            tmpLog.debug('not found')
            return {}
        try:
            with open(jsonFilePath) as jsonFile:
                retDict = json.load(jsonFile)
        except:
            tmpLog.debug('failed to load json')
            return {}
        tmpLog.debug('found')
        return retDict

    # feed events
    # * worker_events.json is put under the access point
    def feed_events(self, workspec, events_dict):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        retVal = True
        if workspec.mapType == WorkSpec.MT_OneToOne:
            # put the json just under the access point
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsFeedFileName)
            tmpLog.debug('feeding events to {0}'.format(jsonFilePath))
            try:
                with open(jsonFilePath, 'w') as jsonFile:
                    json.dump(events_dict, jsonFile)
            except:
                core_utils.dump_error_message(tmpLog)
                retVal = False
        elif workspec.mapType == WorkSpec.MT_MultiJobs:
            # TOBEFIXED
            pass
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsRequestFileName)
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return retVal

    # update events.
    # * the worker needs to put a json under the access point
    def events_to_update(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # look for the json just under the access point
        jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
        readJsonPath = jsonFilePath + suffixReadJson
        # first look for json.read which is not yet acknowledged
        tmpLog.debug('looking for event update file {0}'.format(readJsonPath))
        if os.path.exists(readJsonPath):
            pass
        else:
            tmpLog.debug('looking for event update file {0}'.format(jsonFilePath))
            if not os.path.exists(jsonFilePath):
                # not found
                tmpLog.debug('not found')
                return {}
            try:
                # rename to prevent from being overwritten
                os.rename(jsonFilePath, readJsonPath)
            except:
                tmpLog.error('failed to rename json')
                return {}
        # load json
        try:
            with open(readJsonPath) as jsonFile:
                retDict = json.load(jsonFile)
                newDict = dict()
                # change the key from str to int
                for tmpPandaID, tmpDict in retDict.iteritems():
                    tmpPandaID = long(tmpPandaID)
                    newDict[tmpPandaID] = tmpDict
                retDict = newDict
        except:
            tmpLog.debug('failed to load json')
            return {}
        tmpLog.debug('got {0}'.format(str(retDict)))
        return retDict

    # acknowledge events and files
    # * delete json.read files
    def acknowledge_events_files(self, workspec):
        # get logger
        tmpLog = core_utils.make_logger(_logger, 'workerID={0}'.format(workspec.workerID))
        # remove request file
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonEventsUpdateFileName)
            jsonFilePath += suffixReadJson
            os.remove(jsonFilePath)
        except:
            pass
        try:
            jsonFilePath = os.path.join(workspec.get_access_point(), jsonOutputsFileName)
            jsonFilePath += suffixReadJson
            os.remove(jsonFilePath)
        except:
            pass
        tmpLog.debug('done')
        return


    # setup access points
    def setup_access_points(self, workspec_list):
        for workSpec in workspec_list:
            accessPoint = workSpec.get_access_point()
            # make the dir if missing
            if not os.path.exists(accessPoint):
                os.makedirs(accessPoint)