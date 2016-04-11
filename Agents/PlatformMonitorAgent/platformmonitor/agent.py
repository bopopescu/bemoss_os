# -*- coding: utf-8 -*-
'''
Copyright (c) 2016, Virginia Tech
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the authors and should not be
interpreted as representing official policies, either expressed or implied, of the FreeBSD Project.

This material was prepared as an account of work sponsored by an agency of the United States Government. Neither the
United States Government nor the United States Department of Energy, nor Virginia Tech, nor any of their employees,
nor any jurisdiction or organization that has cooperated in the development of these materials, makes any warranty,
express or implied, or assumes any legal liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or represents that its use would not infringe
privately owned rights.

Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer, or
otherwise does not necessarily constitute or imply its endorsement, recommendation, favoring by the United States
Government or any agency thereof, or Virginia Tech - Advanced Research Institute. The views and opinions of authors
expressed herein do not necessarily state or reflect those of the United States Government or any agency thereof.

VIRGINIA TECH – ADVANCED RESEARCH INSTITUTE
under Contract DE-EE0006352

#__author__ = "BEMOSS Team"
#__credits__ = ""
#__version__ = "2.0"
#__maintainer__ = "BEMOSS Team"
#__email__ = "aribemoss@gmail.com"
#__website__ = "www.bemoss.org"
#__created__ = "2014-09-12 12:04:50"
#__lastUpdated__ = "2016-03-14 11:23:33"
'''

import sys
import json
import logging
import importlib
from volttron.platform.agent import BaseAgent, PublishMixin, periodic
from volttron.platform.agent import utils, matching
from volttron.platform.messaging import headers as headers_mod
import datetime

import psycopg2
import psycopg2.extras
import socket
import settings
from bemoss_lib.utils.catcherror import catcherror

from bemoss_lib.databases.cassandraAPI import cassandraDB
utils.setup_logging()
_log = logging.getLogger(__name__)



# Step1: Agent Initialization
def PlatformMonitorAgent(config_path, **kwargs):
    config = utils.load_config(config_path)

    def get_config(name):
        try:
            kwargs.pop(name)
        except KeyError:
            return config.get(name, '')

    #1. @params agent
    agent_id = get_config('agent_id')
    poll_time = get_config('poll_time')

    db_database = settings.DATABASES['default']['NAME']
    db_host = settings.DATABASES['default']['HOST']
    db_port = settings.DATABASES['default']['PORT']
    db_user = settings.DATABASES['default']['USER']
    db_password = settings.DATABASES['default']['PASSWORD']
    db_table_node_device = settings.DATABASES['default']['TABLE_node_device']
    db_table_device_info = settings.DATABASES['default']['TABLE_device_info']
    db_table_node_info = settings.DATABASES['default']['TABLE_node_info']

    class Agent(PublishMixin, BaseAgent):
        """Agent for querying WeatherUndergrounds API"""

        #1. agent initialization    
        def __init__(self, **kwargs):
            super(Agent, self).__init__(**kwargs)
            #1. initialize all agent variables
            self.variables = kwargs
            self.agent_id = get_config('agent_id')

            try:
                self.con = psycopg2.connect(host=db_host, port=db_port, database=db_database, user=db_user,
                                            password=db_password)
                self.cur = self.con.cursor()  # open a cursor to perfomm database operations
                print("{} connects to the database name {} successfully".format(self.agent_id, db_database))
            except Exception as er:
                print er
                print("ERROR: {} fails to connect to the database name {}".format(self.agent_id, db_database))
                raise


        #2. agent setup method
        def setup(self):
            super(Agent, self).setup()
            self.agentMonitorBehavior()

        #3. deviceMonitorBehavior (TickerBehavior)
        @periodic(poll_time)
        @catcherror('agentMonitoring Failed')
        def agentMonitorBehavior(self):

            self.cur.execute("SELECT device_id,device_type FROM "+db_table_device_info+" WHERE approval_status=%s",
                             ('APR',))
            if self.cur.rowcount != 0:
                rows = self.cur.fetchall()
                for row in rows:
                    self.cur.execute("SELECT previous_zone_id,current_zone_id FROM "+db_table_node_device+" WHERE device_id=%s",
                                     (row[0],))
                    if self.cur.rowcount != 0:
                        zone_info = self.cur.fetchone()
                        new_zone = zone_info[1]
                        old_zone = zone_info[0]
                        self.cur.execute("SELECT node_status FROM "+db_table_node_info+" WHERE  associated_zone=%s",
                             (new_zone,))
                        if self.cur.rowcount != 0:
                            val = self.cur.fetchone()
                            node_status = val[0]
                        else:
                            print "Node for not found for device: " + str(row[0])
                            continue

                        if node_status == "OFFLINE":
                            print "Node is offline. Ignoring the device: "+ str(row[0])
                            continue
                            #network agent should bring the device back to the core

                        topic = '/ui/networkagent/' + str(row[0]) + '/' + str(old_zone) + '/' + str(new_zone) + '/start'+'/APR'
                        headers = {
                            'AgentID': agent_id,
                            headers_mod.CONTENT_TYPE: headers_mod.CONTENT_TYPE.JSON,
                            headers_mod.FROM: agent_id,
                            headers_mod.TO: 'networkagent'
                        }
                        message = ''
                        message = message.encode(encoding='utf_8')
                        self.publish(topic, headers, message)

    Agent.__name__ = 'PlatformMonitorAgent'
    return Agent(**kwargs)

def main(argv=sys.argv):
    '''Main method called by the eggsecutable.'''
    utils.default_main(PlatformMonitorAgent,
                       description='Platform Monitor agent',
                       argv=argv)

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
