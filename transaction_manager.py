from data_manager import data_manager


class transaction_manager():
    def __init__(self):
        print("-------------------- Begin to init TM and DM --------------------")
        print("init Transaction Manager!")
        self.data_manager_nodes = []
        self.instantiate_dm()

    def instantiate_dm(self):
        for i in range(10):
            self.data_manager_nodes.append(data_manager(i))

    def get_instructions(self, instr):
        self.data_manager_nodes[3].get_instructions(instr)
        self.monitor_site_status()

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.isUp:
                pass
            else:
                pass

    def output_site_status(self):
        print("-------------------- Dump all the output --------------------")
        for dm in self.data_manager_nodes:
            if dm.isUp:
                print("Site" + str(dm.idx) + " is up")
            else:
                print("Site" + str(dm.idx) + " is down")

            dm_info = dm.dump()
            print("Site" + str(dm.idx) + ": " + dm_info)
