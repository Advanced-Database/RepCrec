class DataManager:
    def __init__(self, site_idx):
        print("Init Data Manager " + str(site_idx) + "!")
        self.idx = site_idx
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        for v_idx in range(1, 21):
            v_name = "x" + str(v_idx)
            if v_idx % 2 == 0 or v_idx % 10 + 1 == site_idx:
                self.data[v_name] = v_idx * 10

    def get_instructions(self, instr):
        print("Data Manager " + str(self.idx) + " received below instruction:")
        print(instr)

    def dump(self, idx):
        result = "site " + str(idx) + " - "
        for key in sorted(self.data.keys()):
            result += key + ": " + str(self.data[key]) + ", "
        return result

    def detect_fail(self):
        self.is_up = False
        self.lock_table.clear()
