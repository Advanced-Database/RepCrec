from data_manager import data_manager


class transaction_manager():
    def __init__(self):
        print("init transaction manager")
        self.instantiate_dm()

    def instantiate_dm(self):
        data_manager_nodes = []
        for i in range(10):
            data_manager_nodes.append(data_manager(i + 1))

    def get_instructions(self, instr):
        print(instr)
