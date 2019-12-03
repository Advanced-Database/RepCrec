class DataManager:
    def __init__(self, site_id):
        # print("Init Data Manager " + str(site_idx) + "!")
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        self.fail_ts = []
        self.recover_ts = []

        for v_idx in range(1, 21):
            v_name = "x" + str(v_idx)
            if v_idx % 2 == 0 or v_idx % 10 + 1 == self.site_id:
                self.data[v_name] = v_idx * 10
                self.lock_table[v_name] = None, None

    def get_read_lock(self, transaction_id, variable):
        if self.is_up and variable in self.lock_table \
                and (self.lock_table[variable][0] == transaction_id or self.lock_table[variable][1] != 'X'):
            return True
        else:
            return False

    def set_read_lock(self, transaction_id, variable):
        self.lock_table[variable] = transaction_id, 'R'
        return True

    def is_exclusive_lock_conflict(self, transaction_id, variable):
        if self.is_up and variable in self.lock_table:
            if self.lock_table[variable][0] == transaction_id or self.lock_table[variable][1] == None:
                return False
            else:
                return True
        else:
            return False

    def get_exclusive_lock(self, transaction_id, variable):
        if self.is_up and variable in self.lock_table \
                and (self.lock_table[variable][0] == transaction_id or self.lock_table[variable][1] == None):
            return True
        else:
            return False

    def set_exclusive_lock(self, transaction_id, variable):
        self.lock_table[variable] = transaction_id, 'X'
        return True

    def release_locks(self, transaction_id):
        for variable, lock_item in self.lock_table.items():
            if lock_item[0] and lock_item[0] == transaction_id:
                self.lock_table[variable] = None, None
            # print(variable + ": " + str(self.lock_table[variable]))

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
