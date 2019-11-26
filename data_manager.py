class data_manager():
    def __init__(self, idx):
        print("Init Data Manager " + str(idx) + "!")
        self.idx = idx
        self.isUp = True
        self.data = []
        self.lock_table = {}

    def get_instructions(self, instr):
        print("Data Manager " + str(self.idx) + " received below instruction:")
        print(instr)

    def dump(self):
        return "test info"

    def detect_fail(self):
        self.isUp = False
        self.lock_table.clear()
