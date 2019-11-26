from data_manager import DataManager
import random


class TransactionManager:
    def __init__(self):
        print("Init Transaction Manager!")
        self.data_manager_nodes = []
        self.instantiate_dm()

    def instantiate_dm(self):
        for site_num in range(1, 11):
            self.data_manager_nodes.append(DataManager(site_num))
            cur_dm = self.data_manager_nodes[site_num - 1]

            for v_idx in range(1, 21):
                v_name = "x" + str(v_idx)
                if v_idx % 2 == 0 or v_idx % 10 + 1 == site_num:
                    cur_dm.data[v_name] = v_idx * 10

    def get_instructions(self, instr):
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
