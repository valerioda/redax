#!/daq_common/miniconda3/bin/python3
import configparser
import argparse
import os
import daqnt
import json
import time

from MongoConnect import MongoConnect
from DAQController import DAQController


def setup():
    # Parse command line
    parser = argparse.ArgumentParser(description='Manage the DAQ')
    parser.add_argument('--config', type=str, help='Path to your configuration file',
            default='config.ini')
    parser.add_argument('--log', type=str, help='Logging level', default='DEBUG',
            choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'])
    parser.add_argument('--test', action='store_true', help='Are you testing?')
    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config)
    config = config['DEFAULT' if not args.test else "TESTING"]
    daq_config = json.loads(config['MasterDAQConfig'])
    control_mc = daqnt.get_client('daq')
    runs_mc = daqnt.get_client('runs')
    logger = daqnt.get_daq_logger(config['LogName'], level=args.log, mc=control_mc)
    vme_config = json.loads(config['VMEConfig'])

    # Declare necessary classes

    SlackBot = daqnt.DaqntBot(os.environ['SLACK_KEY'])
    logger.info('Dispatcher starting up')
    while True:
        try:
            main(config, control_mc, logger, daq_config, vme_config, SlackBot, runs_mc, args)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as fatal_error:
            logger.debug(fatal_error, exc_info=True)
            logger.error(f'Fatal warning:\tran into {fatal_error}. Try '
                         f'logging error and restart the dispatcher')
            SlackBot.send_message(
                f'Dispatcher just died ({fatal_error}), this is very bad. '
                'We\'re going to try a reboot but please alert the DAQ-group.',
                add_tags='ALL')
            time.sleep(60)
            logger.warning('Restarting main loop')


def main(config, control_mc, logger, daq_config, vme_config, SlackBot, runs_mc, args):
    sh = daqnt.SignalHandler()
    Hypervisor = daqnt.Hypervisor(control_mc[config['ControlDatabaseName']], logger,
                                  daq_config, vme_config,
                                  control_inputs=config['ControlKeys'].split(), sh=sh,
                                  testing=args.test, slackbot=SlackBot)
    MongoConnector = MongoConnect(config, daq_config, logger, control_mc, runs_mc, Hypervisor,
                                  args.test)
    DAQControl = DAQController(config, daq_config, MongoConnector, logger, Hypervisor)
    # connect the triangle
    Hypervisor.mongo_connect = MongoConnector
    Hypervisor.daq_controller = DAQControl

    sleep_period = int(config['PollFrequency'])
    while sh.event.is_set() == False:
        sh.event.wait(sleep_period)
        # Get most recent goal state from database. Users will update this from the website.
        if (goal_state := MongoConnector.get_wanted_state()) is None:
            continue
        # Get the Super-Detector configuration
        current_config = MongoConnector.get_super_detector()
        # Get most recent check-in from all connected hosts
        if (latest_status := MongoConnector.get_update(current_config)) is None:
            continue

        # Print an update
        for detector in latest_status.keys():
            state = 'ACTIVE' if goal_state[detector]['active'] == 'true' else 'INACTIVE'
            msg = (f'The {detector} should be {state} and is '
                    f'{latest_status[detector]["status"].name}')
            if latest_status[detector]['number'] != -1:
                msg += f' ({latest_status[detector]["number"]})'
            logger.debug(msg)
        msg = (f"Linking: tpc-mv: {MongoConnector.is_linked('tpc', 'muon_veto')}, "
               f"tpc-nv: {MongoConnector.is_linked('tpc', 'neutron_veto')}, "
               f"mv-nv: {MongoConnector.is_linked('muon_veto', 'neutron_veto')}")
        logger.debug(msg)

        # Decision time. Are we actually in our goal state? If not what should we do?
        DAQControl.solve_problem(latest_status, goal_state)

    MongoConnector.quit()
    return


if __name__ == '__main__':
    main()
