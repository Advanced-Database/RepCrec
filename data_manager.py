from enum import Enum
from collections import defaultdict


class CommitValue:
    def __init__(self, value, commit_ts):
        self.value = value
        self.commit_ts = commit_ts


class TempValue:
    def __init__(self, value, transaction_id):
        self.value = value
        self.transaction_id = transaction_id


class Variable:
    def __init__(self, variable_id, init_value, is_replicated):
        self.variable_id = variable_id
        self.committed_value_list = [init_value]  # latest commit at front
        self.is_replicated = is_replicated
        self.temp_value = None
        self.is_readable = True

    def get_last_committed_value(self):
        return self.committed_value_list[0].value

    def get_temp_value(self):
        if not self.temp_value:
            raise RuntimeError("No temp value!")
        return self.temp_value.value

    def add_commit_value(self, commit_value):
        self.committed_value_list.insert(0, commit_value)


class Result:
    def __init__(self, success, value=None):
        self.success = success
        self.value = value


class LockType(Enum):
    R = 1
    W = 2


class ReadLock:
    def __init__(self, variable_id, transaction_id):
        self.variable_id = variable_id
        self.transaction_id_set = {transaction_id}
        self.lock_type = LockType.R


class WriteLock:
    def __init__(self, variable_id, transaction_id):
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = LockType.W


class QueuedLock:
    def __init__(self, variable_id, transaction_id, lock_type: LockType):
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = lock_type

    def __repr__(self):
        return "({}, {}, {})".format(
            self.transaction_id, self.variable_id, self.lock_type)


class LockManager:
    def __init__(self, variable_id):
        self.variable_id = variable_id
        self.current_lock = None
        self.queue = []  # list of QueuedLock

    def clear(self):
        self.current_lock = None
        self.queue = []

    # todo: maybe combine set_read_lock, set_write_lock,
    #       and promote_to_write_lock into set_current_lock
    def set_read_lock(self, read_lock):
        # if self.queue:
        #     raise RuntimeError(
        #         "Unresolved queued locks when current lock is None!")
        self.current_lock = read_lock

    def set_write_lock(self, write_lock):
        # if self.queue:
        #     raise RuntimeError(
        #         "Unresolved queued locks when current lock is None!")
        self.current_lock = write_lock

    def promote_to_write_lock(self, write_lock):
        if not self.current_lock:
            raise RuntimeError("No current lock!")
        if not self.current_lock.lock_type == LockType.R:
            raise RuntimeError("Current lock is not R-lock!")
        if len(self.current_lock.transaction_id_set) != 1:
            raise RuntimeError("Other transaction sharing R-lock!")
        if write_lock.transaction_id not in \
                self.current_lock.transaction_id_set:
            raise RuntimeError("{} is not holding current R-lock!".format(
                write_lock.transaction_id))
        self.current_lock = write_lock

    def share_read_lock(self, transaction_id):
        if not self.current_lock.lock_type == LockType.R:
            raise RuntimeError("Attempt to share W-lock!")
        self.current_lock.transaction_id_set.add(transaction_id)

    def add_to_queue(self, new_lock: QueuedLock):
        for queued_lock in self.queue:
            if queued_lock.transaction_id == new_lock.transaction_id:
                if queued_lock.lock_type == new_lock.lock_type or \
                        new_lock.lock_type == LockType.R:
                    return
        self.queue.append(new_lock)

    def has_other_queued_write_lock(self, transaction_id=None):
        for queued_lock in self.queue:
            if queued_lock.lock_type == LockType.W:
                if transaction_id and \
                        queued_lock.transaction_id == transaction_id:
                    continue
                return True
        return False

    def release_current_lock_by_transaction(self, transaction_id):
        if self.current_lock:
            if self.current_lock.lock_type == LockType.R:
                # current lock is R-lock
                if transaction_id in self.current_lock.transaction_id_set:
                    self.current_lock.transaction_id_set.remove(transaction_id)
                if not len(self.current_lock.transaction_id_set):
                    self.current_lock = None
            else:
                # current lock is W-lock
                if self.current_lock.transaction_id == transaction_id:
                    self.current_lock = None


class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        for v_idx in range(1, 21):
            variable_id = "x" + str(v_idx)
            if v_idx % 2 == 0:  # replicated
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), True)
                self.lock_table[variable_id] = LockManager(variable_id)
            elif v_idx % 10 + 1 == self.site_id:  # non-replicated
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), False)
                self.lock_table[variable_id] = LockManager(variable_id)

        self.fail_ts_list = []  # latest fail at end
        self.recover_ts_list = []  # latest recover at end

    def has_variable(self, variable_id):
        return self.data.get(variable_id)

    def read_snapshot(self, variable_id, ts):
        v: Variable = self.data.get(variable_id)
        if v.is_readable:
            for commit_value in v.committed_value_list:
                if commit_value.commit_ts <= ts:
                    if v.is_replicated:
                        for fail_ts in self.fail_ts_list:
                            if commit_value.commit_ts < fail_ts <= ts:
                                return Result(False)
                    return Result(True, commit_value.value)
        return Result(False)

    def read(self, transaction_id, variable_id):
        v: Variable = self.data[variable_id]
        if v.is_readable:
            lm: LockManager = self.lock_table[variable_id]
            current_lock = lm.current_lock
            if current_lock:
                if current_lock.lock_type == LockType.R:
                    if transaction_id in current_lock.transaction_id_set:
                        return Result(True, v.get_last_committed_value())
                    if not lm.has_other_queued_write_lock():
                        lm.share_read_lock(transaction_id)
                        return Result(True, v.get_last_committed_value())
                    # There is a queued W-lock
                    lm.add_to_queue(
                        QueuedLock(variable_id, transaction_id, LockType.R))
                    return Result(False)
                # current_lock is W-lock
                if transaction_id == current_lock.transaction_id:
                    # This transaction holds a W-lock
                    # It has written to the variable
                    # but the new value is not committed yet
                    return Result(True, v.get_temp_value())
                # Another transaction is holding a W-lock
                lm.add_to_queue(
                    QueuedLock(variable_id, transaction_id, LockType.R))
                return Result(False)
            # No existing lock on the variable, create one
            lm.set_read_lock(ReadLock(variable_id, transaction_id))
            return Result(True, v.get_last_committed_value())
        return Result(False)

    def get_write_lock(self, transaction_id, variable_id):
        lm: LockManager = self.lock_table[variable_id]
        current_lock = lm.current_lock
        if current_lock:
            if current_lock.lock_type == LockType.R:
                if len(current_lock.transaction_id_set) != 1:
                    # Multiple transactions holding R-lock on the same variable
                    lm.add_to_queue(
                        QueuedLock(variable_id, transaction_id, LockType.W))
                    return False
                # Only one transaction holding an R-lock
                # Which one?
                if transaction_id in current_lock.transaction_id_set:
                    # Only this transaction holds the R-lock
                    # Can it be promoted to W-lock?
                    if lm.has_other_queued_write_lock(transaction_id):
                        lm.add_to_queue(
                            QueuedLock(variable_id, transaction_id, LockType.W))
                        return False
                    return True
                # One other transaction is holding the R-lock
                lm.add_to_queue(
                    QueuedLock(variable_id, transaction_id, LockType.W))
                return False
            # current lock is W-lock
            if transaction_id == current_lock.transaction_id:
                # This transaction already holds a W-lock
                return True
            # Another transaction is holding W-lock
            lm.add_to_queue(
                QueuedLock(variable_id, transaction_id, LockType.W))
            return False
        # No existing lock on the variable
        return True

    def write(self, transaction_id, variable_id, value):
        v: Variable = self.data[variable_id]
        lm: LockManager = self.lock_table[variable_id]
        current_lock = lm.current_lock
        if current_lock:
            if current_lock.lock_type == LockType.R:
                if len(current_lock.transaction_id_set) != 1:
                    raise RuntimeError("Cannot promote to W-Lock: "
                                       "other transactions are holding R-lock!")
                if transaction_id in current_lock.transaction_id_set:
                    if lm.has_other_queued_write_lock(transaction_id):
                        raise RuntimeError("Cannot promote to W-Lock: "
                                           "other R-lock is waiting in queue!")
                    lm.promote_to_write_lock(
                        WriteLock(variable_id, transaction_id))
                    v.temp_value = TempValue(value, transaction_id)
                    return
                raise RuntimeError("Cannot promote to W-Lock: "
                                   "R-lock is not held by this transaction!")
            # current lock is W-lock
            if transaction_id == current_lock.transaction_id:
                # This transaction already holds a W-lock
                v.temp_value = TempValue(value, transaction_id)
                return
            # Another transaction is holding W-lock
            raise RuntimeError("Cannot get W-Lock: "
                               "another transaction is holding W-lock!")
        # No existing lock on the variable
        lm.set_write_lock(WriteLock(variable_id, transaction_id))
        v.temp_value = TempValue(value, transaction_id)

    def dump(self, idx):
        result = "site " + str(idx) + " - "
        for key in self.data.keys():
            result += key + ": " + \
                      str(self.data[key].committed_value_list[0].value) + ", "
        return result

    def abort(self, transaction_id):
        for lm in self.lock_table.values():
            # release current lock held by this transaction
            lm.release_current_lock_by_transaction(transaction_id)
            # remove queued locks of this transaction
            for ql in list(lm.queue):
                if ql.transaction_id == transaction_id:
                    lm.queue.remove(ql)
        self.resolve_lock_table()

    def commit(self, transaction_id, commit_ts):
        for lm in self.lock_table.values():
            # release current lock held by this transaction
            lm.release_current_lock_by_transaction(transaction_id)
            # there shouldn't be any queued locks of this transaction
            # print(lm.queue)
            for ql in list(lm.queue):
                if ql.transaction_id == transaction_id:
                    raise RuntimeError(
                        "{} cannot commit with unresolved queued locks!".format(
                            transaction_id))
        # commit temp values
        for v in self.data.values():
            if v.temp_value and v.temp_value.transaction_id == transaction_id:
                v.add_commit_value(CommitValue(v.temp_value.value, commit_ts))
                v.is_readable = True
        self.resolve_lock_table()

    def resolve_lock_table(self):
        for v, lm in self.lock_table.items():
            if lm.queue:
                if not lm.current_lock:
                    # current lock is None
                    # pop the first queued lock and add to
                    first_ql = lm.queue.pop(0)
                    if first_ql.lock_type == LockType.R:
                        lm.set_read_lock(ReadLock(
                            first_ql.variable_id, first_ql.transaction_id))
                    else:
                        lm.set_write_lock(WriteLock(
                            first_ql.variable_id, first_ql.transaction_id))
                if lm.current_lock.lock_type == LockType.R:
                    # current lock is R-lock
                    # share R-lock with leading R-queued-locks
                    for ql in list(lm.queue):
                        if ql.lock_type == LockType.W:
                            if len(lm.current_lock.transaction_id_set) == 1 \
                                    and ql.transaction_id in \
                                    lm.current_lock.transaction_id_set:
                                lm.promote_to_write_lock(WriteLock(
                                    ql.variable_id, ql.transaction_id))
                                lm.queue.remove(ql)
                            break
                        lm.share_read_lock(ql.transaction_id)
                        lm.queue.remove(ql)

    def fail(self, ts):
        self.is_up = False
        self.fail_ts_list.append(ts)
        for lm in self.lock_table.values():
            lm.clear()

    def recover(self, ts):
        self.is_up = True
        self.recover_ts_list.append(ts)
        for v in self.data.values():
            if v.is_replicated:
                v.is_readable = False

    def generate_blocking_graph(self):
        def current_blocks_queued(current_lock, queued_lock):
            if current_lock.lock_type == LockType.R:
                if queued_lock.lock_type.R or \
                        (len(current_lock.transaction_id_set) == 1 and
                         queued_lock.transaction_id in
                         current_lock.transaction_id_set):
                    return False
                return True
            # current lock is W-lock
            return not current_lock.transaction_id == queued_lock.transaction_id

        def queued_blocks_queued(queued_lock_left, queued_lock_right):
            if queued_lock_left.lock_type == LockType.R and \
                    queued_lock_right.lock_type.R:
                return False
            # at least one lock is W-lock
            return not queued_lock_left.transaction_id == queued_lock_right.transaction_id

        graph = defaultdict(set)
        for variable_id, lm in self.lock_table.items():
            if not lm.current_lock or not lm.queue:
                continue
            for ql in lm.queue:
                if current_blocks_queued(lm.current_lock, ql):
                    if lm.current_lock.lock_type == LockType.R:
                        for t_id in lm.current_lock.transaction_id_set:
                            if t_id != ql.transaction_id:
                                graph[ql.transaction_id].add(t_id)
                    else:
                        if lm.current_lock.transaction_id != ql.transaction_id:
                            graph[ql.transaction_id].add(
                                lm.current_lock.transaction_id)
            for i in range(len(lm.queue)):
                for j in range(i):
                    if not queued_blocks_queued(lm.queue[j], lm.queue[i]):
                        # if lm.queue[j].transaction_id != lm.queue[i
                        # ].transaction_id:
                        graph[lm.queue[i].transaction_id].add(
                            lm.queue[j].transaction_id)
        # print("graph {}={}".format(self.site_id, dict(graph)))
        return graph
