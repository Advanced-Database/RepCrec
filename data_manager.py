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
        self.fail_ts = []
        self.recover_ts = []

    def get_read_lock(self, variable):
        if self.is_up and variable in self.lock_table and \
                self.lock_table[variable] != 'x':
            return True
        else:
            return False

    def read(self, variable):
        return self.data[variable]

    def set_read_lock(self, variable):
        self.lock_table[variable] = 'r'

    def get_exclusive_lock(self, variable):
        if self.is_up and variable in self.lock_table and \
                self.lock_table[variable] is None:
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

    def fail(self, ts):
        self.is_up = False
        self.fail_ts.append(ts)
        self.lock_table.clear()

    def recover(self, ts):
        self.is_up = True
        self.recover_ts.append(ts)
        # todo:
        #   non-replicated: available to read and write.
        #   replicated: Allow writes, Reject reads until a write has occurred.
