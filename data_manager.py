from enum import Enum


class CommitValue:
    def __init__(self, value, commit_ts):
        self.value = value
        self.commit_ts = commit_ts


class Variable:
    def __init__(self, variable_id, init_value, is_replicated):
        self._variable_id = variable_id
        self.committed_value_list = [init_value]  # latest commit at front
        self.is_replicated = is_replicated
        self.temp_value = None

    def get_last_committed_value(self):
        return self.committed_value_list[0].value

    def get_temp_value(self):
        if not self.temp_value:
            raise RuntimeError("No temp value!")
        return self.temp_value


class Result:
    def __init__(self, success, value=None):
        self.success = success
        self.value = value


class LockType(Enum):
    R = 1
    W = 2


class QueuedLock:
    def __init__(self, variable_id, transaction_id, lock_type: LockType):
        self._variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = lock_type


class Lock:
    def __init__(self, variable_id, transaction_id, lock_type: LockType):
        self._variable_id = variable_id
        self.transaction_list = {transaction_id}
        self.lock_type = lock_type
        self.queue = []

    def share_read(self, transaction_id):
        if not self.lock_type == LockType.R:
            raise RuntimeError("Attempt to share W-lock!")
        self.transaction_list.add(transaction_id)

    def add_to_queue(self, queued_lock):
        self.queue.append(queued_lock)

    def has_queued_write_lock(self):
        for queued_lock in self.queue:
            if queued_lock.lock_type == LockType.W:
                return True
        return False


class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        for v_idx in range(1, 21):
            variable_id = "x" + str(v_idx)
            if v_idx % 2 == 0:
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), True)
                # self.lock_table[variable_id] = None
            elif v_idx % 10 + 1 == self.site_id:
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), False)
                # self.lock_table[variable_id] = None

        self.fail_ts_list = []  # latest fail at end
        self.recover_ts_list = []  # latest recover at end
        self.uncommitted_write_list = {}

    def read_snapshot(self, variable_id, ts):
        v = self.data.get(variable_id)
        if v:
            for commit_value in v.committed_value_list:
                if commit_value.commit_ts <= ts:
                    if v.is_replicated:
                        for fail_ts in self.fail_ts_list:
                            if commit_value.commit_ts < fail_ts <= ts:
                                return Result(False)
                    return Result(True, commit_value.value)
        return Result(False)

    def read(self, transaction_id, variable_id):
        v = self.data.get(variable_id)
        if v:
            lock = self.lock_table.get(variable_id)
            if lock:
                if lock.lock_type == LockType.R:
                    if transaction_id in lock.transaction_list:
                        return Result(True, v.get_last_committed_value())
                    if not lock.has_queued_write_lock():
                        lock.share_read(transaction_id)
                        return Result(True, v.get_last_committed_value())
                    # There is a queued W-lock
                    lock.add_to_queue(
                        QueuedLock(variable_id, transaction_id, LockType.R))
                    return Result(False)
                # lock.lock_type == LockType.W
                if len(lock.transaction_list) != 1:
                    raise RuntimeError("Multiple transaction holding W-lock"
                                       " on the same variable!")
                if transaction_id in lock.transaction_list:
                    # This transaction holds a W-lock
                    # It has written to the variable
                    # but the new value is not committed yet
                    return Result(True, v.get_temp_value())
                # Another transaction is holding a W-lock
                lock.add_to_queue(
                    QueuedLock(variable_id, transaction_id, LockType.W))
                return Result(False)
            # No existing lock on the variable, create one
            self.lock_table[variable_id] = Lock(
                variable_id, transaction_id, LockType.R)
            return Result(True, v.get_last_committed_value())
        return Result(False)

    def write(self, transaction_id, variable_id, value):
        pass

    def dump(self, idx):
        result = "site " + str(idx) + " - "
        for key in sorted(self.data.keys()):
            result += key + ": " + \
                      str(self.data[key].committed_value_list[0].value) + ", "
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
