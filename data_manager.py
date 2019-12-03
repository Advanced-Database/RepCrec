class DataManager:
    def __init__(self, site_id):
        # print("Init Data Manager " + str(site_idx) + "!")
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}

        for v_idx in range(1, 21):
            v_name = "x" + str(v_idx)
            if v_idx % 2 == 0 or v_idx % 10 + 1 == self.site_id:
                self.data[v_name] = v_idx * 10
                self.lock_table[v_name] = None

    def get_instructions(self, instr):
        print("Data Manager " + str(self.site_id) +
              " received below instruction:")
        print(instr)

    def get_read_lock(self, variable):
        if self.is_up and variable in self.lock_table and self.lock_table[variable] != 'x':
            return True
        else:
            return False

    def set_read_lock(self, variable):
        self.lock_table[variable] = 'r'

    def get_exclusive_lock(self, variable):
        if self.is_up and variable in self.lock_table and self.lock_table[variable] == None:
            return True
        else:
            return False

    def set_exclusive_lock(self, variable):
        self.lock_table[variable] = 'x'

    def dump(self, idx):
        result = "site " + str(idx) + " - "
        for key in sorted(self.data.keys()):
            result += key + ": " + str(self.data[key]) + ", "
        return result

    def detect_fail(self):
        self.is_up = False
        self.lock_table.clear()
