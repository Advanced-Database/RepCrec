class CommitValue:
    def __init__(self, value, commit_ts):
        self.value = value
        self.commit_ts = commit_ts


class Variable:
    def __init__(self, v_name, init_commit_value, is_replicated):
        self.v_name = v_name
        self.commit_value_list = [init_commit_value]  # latest commit at front
        self.is_replicated = is_replicated


class Result:
    def __init__(self, success, value=None):
        self.success = success
        self.value = value


class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        for v_idx in range(1, 21):
            v_name = "x" + str(v_idx)
            if v_idx % 2 == 0:
                self.data[v_name] = Variable(
                    v_name, CommitValue(v_idx * 10, 0), True)
                self.lock_table[v_name] = None
            elif v_idx % 10 + 1 == self.site_id:
                self.data[v_name] = Variable(
                    v_name, CommitValue(v_idx * 10, 0), False)
                self.lock_table[v_name] = None

        self.fail_ts_list = []  # latest fail at end
        self.recover_ts_list = []  # latest recover at end

    def read_snapshot(self, variable, ts):
        if self.data.get(variable):
            for commit_value in self.data[variable].commit_value_list:
                if commit_value.commit_ts <= ts:
                    if self.data[variable].is_replicated:
                        for fail_ts in self.fail_ts_list:
                            if commit_value.commit_ts < fail_ts <= ts:
                                return Result(False)
                    return Result(True, commit_value.value)
        return Result(False)

    def get_read_lock(self, variable):
        if self.is_up and variable in self.lock_table and \
                self.lock_table[variable] != 'x':
            self.lock_table[variable] = 'r'
            return True
        else:
            return False

    def read(self, variable):
        return self.data[variable]

    def get_exclusive_lock(self, variable):
        if self.is_up and variable in self.lock_table and \
                self.lock_table[variable] is None:
            self.lock_table[variable] = 'x'
            return True
        else:
            return False

    def dump(self, idx):
        result = "site " + str(idx) + " - "
        for key in sorted(self.data.keys()):
            result += key + ": " +\
                      str(self.data[key].commit_value_list[0].value) + ", "
        return result

    def fail(self, ts):
        self.is_up = False
        self.fail_ts_list.append(ts)
        self.lock_table.clear()

    def recover(self, ts):
        self.is_up = True
        self.recover_ts_list.append(ts)
        # todo:
        #   non-replicated: available to read and write.
        #   replicated: Allow writes, Reject reads until a write has occurred.
