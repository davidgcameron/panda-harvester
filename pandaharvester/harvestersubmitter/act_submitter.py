import logging

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase

from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProxy import aCTProxy
from act.panda.aCTDBPanda import aCTDBPanda


# submitter for aCT
class ACTSubmitter(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        # TODO: use harvester logging
        logging.basicConfig(level=logging.DEBUG)
        self.log = logging.logger()

        # Set up aCT DB connection
        self.conf = aCTConfigARC()
        self.actDB = aCTDBPanda(self.log, self.conf.get(["db", "file"]))

        # Set up proxy map (prod/pilot roles)
        actp = aCTProxy.aCTProxy(self.log)
        for role in self.conf.getList(['voms', 'roles', 'item']):
            attr = '/atlas/Role='+role
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception("Proxy with DN "+dn+" and attribute "+attr+" was not found in proxies table")

            proxyfile = actp.path(dn, attribute=attr)
            # pilot role is mapped to analysis type
            if role == 'pilot':
                role = 'analysis'
            self.proxymap[role] = proxyid


    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:

            # Assume for aCT that jobs are always pre-fetched (no late-binding)
            for jobSpec in workSpec.get_jobspec_list():

                desc = {}
                desc['pandastatus'] = 'sent'
                desc['actpandastatus'] = 'sent'
                desc['siteName'] = jobSpec.computingSite
                desc['proxyid'] = self.proxymap["TODO: how to get prod/analy queue"]

                try:
                    self.actDB.insertJob(jobSpec.PandaID, jobSpec.jobParams, desc)
                    result = (True, '')
                except Exception as e:
                    result = (False, "Failed to insert job into aCT DB: %s" % str(e))

                retList.append(result)

        return retList
