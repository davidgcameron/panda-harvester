"""
Work spec class

"""

import re
import datetime
from spec_base import SpecBase


# work spec
class WorkSpec(SpecBase):
    # worker statuses
    ST_submitted = 'submitted'
    ST_running = 'running'
    ST_finished = 'finished'
    ST_failed = 'failed'
    ST_ready = 'ready'
    ST_cancelled = 'cancelled'

    # list of worker statuses
    ST_LIST = [ST_submitted,
               ST_running,
               ST_finished,
               ST_failed,
               ST_ready,
               ST_cancelled]

    # type of mapping between job and worker
    MT_NoJob = 'NoJob'
    MT_OneToOne = 'OneToOne'
    MT_MultiJobs = 'MultiJobs'
    MT_MultiWorkers = 'MultiWorkers'

    # events
    EV_noEvents = 0
    EV_useEvents = 1
    EV_requestEvents = 2

    # attributes
    attributesWithTypes = ('workerID:integer',
                           'batchID:text',
                           'mapType:text',
                           'queueName:text',
                           'status:text',
                           'hasJob:integer',
                           'workParams:blob',
                           'workAttributes:blob',
                           'eventsRequestParams:blob',
                           'eventsRequest:integer',
                           'computingSite:text',
                           'creationTime:timestamp',
                           'submitTime:timestamp',
                           'startTime:timestamp',
                           'endTime:timestamp',
                           'nCore:integer',
                           'walltime:timestamp',
                           'accessPoint:text',
                           'modificationTime:timestamp',
                           'lastUpdate:timestamp',
                           'eventFeedTime:timestamp',
                           'lockedBy:text',
                           'postProcessed:integer',
                           'nodeID:text',
                           'minRamCount:integer',
                           'maxDiskCount:integer',
                           'maxWalltime:integer',
                           'killTime:timestamp'
                           )

    # constructor
    def __init__(self):
        SpecBase.__init__(self)
        object.__setattr__(self, 'isNew', False)
        object.__setattr__(self, 'nextLookup', False)
        object.__setattr__(self, 'jobspec_list', None)
        object.__setattr__(self, 'pandaid_list', None)

    # set status
    def set_status(self, value):
        if self.status != value:
            self.trigger_propagation()
        self.status = value

    # get access point
    def get_access_point(self):
        # replace placeholders
        if '$' in self.accessPoint:
            patts = re.findall('\$\{([a-zA-Z\d]+)\}', self.accessPoint)
            for patt in patts:
                if hasattr(self, patt):
                    tmpVar = str(getattr(self, patt))
                    tmpKey = '${' + patt + '}'
                    self.accessPoint = self.accessPoint.replace(tmpKey, tmpVar)
        return self.accessPoint

    # set job spec list
    def set_jobspec_list(self, jobspec_list):
        self.jobspec_list = jobspec_list

    # get job spec list
    def get_jobspec_list(self):
        return self.jobspec_list

    # convert worker status to job status
    def convert_to_job_status(self, status=None):
        if status is None:
            status = self.status
        if status in [self.ST_submitted, self.ST_ready]:
            jobStatus = 'starting'
            jobSubStatus = status
        elif status in [self.ST_finished, self.ST_failed, self.ST_cancelled]:
            jobStatus = status
            jobSubStatus = 'to_transfer'
        else:
            jobStatus = 'running'
            jobSubStatus = status
        return jobStatus, jobSubStatus

    # check if post processed
    def is_post_processed(self):
        return self.postProcessed == 1

    # set post processed flag
    def post_processed(self):
        self.postProcessed = 1

    # trigger next lookup
    def trigger_next_lookup(self):
        self.nextLookup = True
        self.modificationTime = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    # trigger propagation
    def trigger_propagation(self):
        self.lastUpdate = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

    # disable propagation
    def disable_propagation(self):
        self.lastUpdate = None
        self.force_update('lastUpdate')

    # final status
    def is_final_status(self):
        return self.status in [self.ST_finished, self.ST_failed, self.ST_cancelled]

    # convert to propagate
    def convert_to_propagate(self):
        data = dict()
        for attr in ['workerID',
                     'batchID',
                     'queueName',
                     'status',
                     'computingSite',
                     'nCore',
                     'nodeID',
                     'submitTime',
                     'startTime',
                     'endTime'
                     ]:
            val = getattr(self, attr)
            if val is not None:
                if isinstance(val, datetime.datetime):
                    val = 'datetime/' + val.strftime('%Y-%m-%d %H:%M:%S.%f')
                data[attr] = val
        if self.pandaid_list is not None:
            data['pandaid_list'] = self.pandaid_list
        return data
