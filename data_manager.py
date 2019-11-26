class DataManager:
    def __init__(self, idx):
        print("Init Data Manager " + str(idx) + "!")
        self.idx = idx
        self.is_up = True
        self.data = []
        self.lock_table = {}

    def get_instructions(self, instr):
        print("Data Manager " + str(self.idx) + " received below instruction:")
        print(instr)

    def dump(self):
        return "test info"

    def detect_fail(self):
        self.is_up = False
        self.lock_table.clear()
