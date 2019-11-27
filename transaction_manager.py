from data_manager import DataManager
import random


class TransactionManager:
    def __init__(self):
        print("Init Transaction Manager!")
        self.data_manager_nodes = []
        for site_idx in range(1, 11):
            self.data_manager_nodes.append(DataManager(site_idx))

    def parse_instruction(self, instr):
        ran_site = random.randrange(0, 10)
        self.data_manager_nodes[ran_site].get_instructions(instr)
        self.monitor_site_status()

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.is_up:
                pass
            else:
                pass

    def output_site_status(self):
        print("-------------------- Dump all the output --------------------")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.idx) + " is up")
            else:
                print("Site" + str(dm.idx) + "'s status: Down")

            dm_info = dm.dump(dm.idx)
            print("Site" + str(dm.idx) + "'s data: " + dm_info)

    def fail(self):
        pass

    def recover(self):
        pass
