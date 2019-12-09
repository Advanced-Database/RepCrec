from enum import Enum
from collections import defaultdict


class CommitValue:
    def __init__(self, value, commit_ts):
        '''
        initiate the CommitValue object
        :param value: commit's value
        :param commit_ts: commit's time
        '''
        self.value = value
        self.commit_ts = commit_ts


# save the temporary written value before committing the transaction
class TempValue:
    def __init__(self, value, transaction_id):
        '''
        initiate the TempValue object
        :param value: a temporary value written by the transaction holding W-lock
        :param transaction_id: the id of the transaction holding W-lock
        '''
        self.value = value
        self.transaction_id = transaction_id


class Variable:
    def __init__(self, variable_id, init_value, is_replicated):
        '''
        initiate the Variable object
        :param variable_id: variable's identifier
        :param init_value: variable's initialized value
        :param is_replicated: indicate if variable is replicated type or not
        '''
        self.variable_id = variable_id
        self.committed_value_list = [init_value]  # latest commit at front
        self.temp_value = None
        self.is_replicated = is_replicated  # even indexed are replicated
        self.is_readable = True  # replicated var not readable at site recovery

    def get_last_committed_value(self):
        '''
        :return: the latest committed value
        '''
        return self.committed_value_list[0].value

    def get_temp_value(self):
        '''
        :return: the temporary value written by a transaction holding a W-lock
        '''
        if not self.temp_value:
            raise RuntimeError("No temp value!")
        return self.temp_value.value

    def add_commit_value(self, commit_value):
        '''
        add a new CommitValue object into the front of the committed value list
        :param commit_value: a CommitValue object
        '''
        self.committed_value_list.insert(0, commit_value)


class Result:
    def __init__(self, success, value=None):
        '''
        initiate the Result object
        :param success: indicate if the result is successful or not
        :param value: result's value
        '''
        self.success = success
        self.value = value


class LockType(Enum):
    R = 1
    W = 2


class ReadLock:
    def __init__(self, variable_id, transaction_id):
        '''
        initiate the ReadLock object
        :param variable_id: variable's id for the R-lock
        :param transaction_id: transaction's id for the R-lock
        '''
        self.variable_id = variable_id
        # multiple transactions could share a R-lock
        self.transaction_id_set = {transaction_id}
        self.lock_type = LockType.R

    def __repr__(self):
        return "({}, {}, {})".format(
            self.transaction_id_set, self.variable_id, self.lock_type)


class WriteLock:
    def __init__(self, variable_id, transaction_id):
        '''
        initiate the WriteLock object
        :param variable_id: variable's id for the W-lock
        :param transaction_id: transaction's id for the W-lock
        '''
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = LockType.W

    def __repr__(self):
        return "({}, {}, {})".format(
            self.transaction_id, self.variable_id, self.lock_type)


class QueuedLock:
    def __init__(self, variable_id, transaction_id, lock_type: LockType):
        '''
        initiate the QueuedLock object
        :param variable_id: variable's id for the Q-lock
        :param transaction_id: transaction's id for the Q-lock
        :param lock_type: either R or W type of lock for this Q-lock
        '''
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = lock_type  # Q-lock could be either read or write

    def __repr__(self):
        return "({}, {}, {})".format(
            self.transaction_id, self.variable_id, self.lock_type)


class LockManager:
    def __init__(self, variable_id):
        '''
        initiate the LockManager object
        :param variable_id: an variable's id for a lock manager
        '''
        self.variable_id = variable_id
        self.current_lock = None
        self.queue = []  # list of QueuedLock

    def clear(self):
        '''
        clean the current lock and the lock queue for an variable
        '''
        self.current_lock = None
        self.queue = []

    def set_current_lock(self, lock):
        '''
        set a new lock as the current lock
        :param lock: either R or W-lock
        '''
        self.current_lock = lock

    def promote_current_lock(self, write_lock):
        '''
        promote the current lock from read to write for the same transaction
        :param write_lock: the new W-lock
        '''
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
        self.set_current_lock(write_lock)

    def share_read_lock(self, transaction_id):
        '''
        share the same R-lock among multiple transactions
        :param transaction_id: the id of the transaction wanting to share the R-lock
        '''
        if not self.current_lock.lock_type == LockType.R:
            raise RuntimeError("Attempt to share W-lock!")
        self.current_lock.transaction_id_set.add(transaction_id)

    def add_to_queue(self, new_lock: QueuedLock):
        '''
        add the new lock into lock's queue
        :param new_lock: the new lock
        '''
        for queued_lock in self.queue:
            if queued_lock.transaction_id == new_lock.transaction_id:
                # transaction holds the same type of lock or the new lock is
                # a R-lock when already had locks in queue
                if queued_lock.lock_type == new_lock.lock_type or \
                        new_lock.lock_type == LockType.R:
                    return
        self.queue.append(new_lock)

    def has_other_queued_write_lock(self, transaction_id=None):
        '''
        check if there's any other W-lock exists in the lock's queue
        :param transaction_id: transaction's id
        :return: boolean value to indicate if existing queued W-lock
        '''
        for queued_lock in self.queue:
            if queued_lock.lock_type == LockType.W:
                if transaction_id and \
                        queued_lock.transaction_id == transaction_id:
                    continue
                return True
        return False

    def release_current_lock_by_transaction(self, transaction_id):
        '''
        release the current lock depending on different cases of transaction
        :param transaction_id:
        '''
        if self.current_lock:
            if self.current_lock.lock_type == LockType.R:
                # current lock is R-lock
                if transaction_id in self.current_lock.transaction_id_set:
                    self.current_lock.transaction_id_set.remove(transaction_id)
                if not len(self.current_lock.transaction_id_set):
                    # release when no other transaction holding R-lock
                    self.current_lock = None
            else:
                # current lock is W-lock
                if self.current_lock.transaction_id == transaction_id:
                    self.current_lock = None


class DataManager:
    def __init__(self, site_id):
        '''
        initiate the DataManager object
        :param site_id: the id of the site managed by this data manager
        '''
        self.site_id = site_id  # int type
        self.is_up = True
        self.data = {}  # store each variable
        self.lock_table = {}  # store lock manager for each variable
        self.fail_ts_list = []  # latest fail at end
        self.recover_ts_list = []  # latest recover at end

        for v_idx in range(1, 21):
            variable_id = "x" + str(v_idx)
            if v_idx % 2 == 0:  # replicated (even)
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), True)
                self.lock_table[variable_id] = LockManager(variable_id)
            elif v_idx % 10 + 1 == self.site_id:  # non-replicated (odd)
                self.data[variable_id] = Variable(
                    variable_id, CommitValue(v_idx * 10, 0), False)
                self.lock_table[variable_id] = LockManager(variable_id)

    def has_variable(self, variable_id):
        '''
        check if having a variable in this site
        :param variable_id: variable's id
        :return: boolean value to indicate if existing the variable in this site
        '''
        return self.data.get(variable_id)

    def read_snapshot(self, variable_id, ts):
        '''
        read the snapshot of a variable's value (multiversion) for read-only transaction
        :param variable_id: variable's id
        :param ts: the beginning time of this transaction
        :return: the result of the read action
        '''
        v: Variable = self.data.get(variable_id)
        if v.is_readable:
            for commit_value in v.committed_value_list:
                # find the latest commit value before the transaction's begin
                if commit_value.commit_ts <= ts:
                    # only replicated variables need to be handled
                    if v.is_replicated:
                        for fail_ts in self.fail_ts_list:
                            # if the site has failed after the commit and
                            # before the transaction begins
                            if commit_value.commit_ts < fail_ts <= ts:
                                return Result(False)
                    return Result(True, commit_value.value)
        return Result(False)

    def read(self, transaction_id, variable_id):
        '''
        read a variable's value for non-read-only transaction
        :param transaction_id: transaction's id
        :param variable_id: variable's id
        :return: the result of the read action
        '''
        v: Variable = self.data.get(variable_id)
        if v.is_readable:  # avoid the revovery case
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
            lm.set_current_lock(ReadLock(variable_id, transaction_id))
            return Result(True, v.get_last_committed_value())
        return Result(False)

    def get_write_lock(self, transaction_id, variable_id):
        '''
        check if allowing to get W-lock from this site
        :param transaction_id: transaction's id
        :param variable_id: variable's id
        :return: boolean value to indicate if allowing to get W-lock from this site
        '''
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
        '''
        write a new value into the variable through the transaction
        :param transaction_id: transaction's id
        :param variable_id: variable's id
        :param value: new value
        '''
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
                                           "other W-lock is waiting in queue!")
                    lm.promote_current_lock(
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
        lm.set_current_lock(WriteLock(variable_id, transaction_id))
        v.temp_value = TempValue(value, transaction_id)

    def dump(self):
        '''
        standard output current status of all the variables of this site, and the up and down of the site
        '''
        if self.is_up:
            print("Site {} [UP]:".format(self.site_id))
        else:
            print("Site {} [DOWN]:".format(self.site_id))

        replicated = ""
        non_replicated = ""
        for v in self.data.values():
            v_str = "{}: {}, ".format(v.variable_id,
                                      v.get_last_committed_value())
            if v.is_replicated:
                replicated += v_str
            else:
                non_replicated += v_str
        print("     " + replicated)
        if non_replicated:
            print("     " + non_replicated)

    def abort(self, transaction_id):
        '''
        abort the transaction and remove its locks
        :param transaction_id: transaction's id
        '''
        for lm in self.lock_table.values():
            # release current lock held by this transaction
            lm.release_current_lock_by_transaction(transaction_id)
            # remove queued locks of this transaction
            for ql in list(lm.queue):
                if ql.transaction_id == transaction_id:
                    lm.queue.remove(ql)
        self.resolve_lock_table()

    def commit(self, transaction_id, commit_ts):
        '''
        commit the written value into variable and remove the transaction's locks
        :param transaction_id: transaction's id
        :param commit_ts: commit's time
        '''
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
        '''
        recheck the lock table and perform the necessary lock setting or lock sharing action
        '''
        for v, lm in self.lock_table.items():
            if lm.queue:
                if not lm.current_lock:
                    # current lock is None
                    # pop the first queued lock and add to
                    first_ql = lm.queue.pop(0)
                    if first_ql.lock_type == LockType.R:
                        lm.set_current_lock(ReadLock(
                            first_ql.variable_id, first_ql.transaction_id))
                    else:
                        lm.set_current_lock(WriteLock(
                            first_ql.variable_id, first_ql.transaction_id))
                if lm.current_lock.lock_type == LockType.R:
                    # current lock is R-lock
                    # share R-lock with leading R-queued-locks
                    for ql in list(lm.queue):
                        if ql.lock_type == LockType.W:
                            if len(lm.current_lock.transaction_id_set) == 1 \
                                    and ql.transaction_id in \
                                    lm.current_lock.transaction_id_set:
                                lm.promote_current_lock(WriteLock(
                                    ql.variable_id, ql.transaction_id))
                                lm.queue.remove(ql)
                            break
                        lm.share_read_lock(ql.transaction_id)
                        lm.queue.remove(ql)

    def fail(self, ts):
        '''
        update the site with failed status and clean the lock table
        :param ts: record the failure time
        '''
        self.is_up = False
        self.fail_ts_list.append(ts)
        for lm in self.lock_table.values():
            lm.clear()

    def recover(self, ts):
        '''
        update the site with recovered status and clean the lock table
        :param ts: record the recovery time
        '''
        self.is_up = True
        self.recover_ts_list.append(ts)
        for v in self.data.values():
            if v.is_replicated:
                v.is_readable = False  # only for replicated variables

    def generate_blocking_graph(self):
        '''
        generate the blocking graph for this site
        :return:
        '''
        def current_blocks_queued(current_lock, queued_lock):
            '''
            check if current blocks queued
            :param current_lock: current lock for the variable
            :param queued_lock: each queued lock for the variable
            :return: boolean value to indicate if current blocks queued
            '''
            if current_lock.lock_type == LockType.R:
                if queued_lock.lock_type == LockType.R or \
                        (len(current_lock.transaction_id_set) == 1 and
                         queued_lock.transaction_id in
                         current_lock.transaction_id_set):
                    return False
                return True
            # current lock is W-lock
            return not current_lock.transaction_id == queued_lock.transaction_id

        def queued_blocks_queued(queued_lock_left, queued_lock_right):
            '''
            check if queued blocks queued
            :param queued_lock_left: the left lock of the queue
            :param queued_lock_right: the right lock of the queue
            :return: boolean value to indicate if queued blocks queued
            '''
            if queued_lock_left.lock_type == LockType.R and \
                    queued_lock_right.lock_type == LockType.R:
                return False
            # at least one lock is W-lock
            return not queued_lock_left.transaction_id == queued_lock_right.transaction_id

        graph = defaultdict(set)
        for variable_id, lm in self.lock_table.items():
            if not lm.current_lock or not lm.queue:
                continue
            # print("current_lock: {}".format(lm.current_lock))
            # print("queue: {}".format(lm.queue))
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
                    # print("queued_blocks_queued({}, {})".format(lm.queue[j], lm.queue[i]))
                    if queued_blocks_queued(lm.queue[j], lm.queue[i]):
                        # if lm.queue[j].transaction_id != lm.queue[i
                        # ].transaction_id:
                        graph[lm.queue[i].transaction_id].add(
                            lm.queue[j].transaction_id)
        # print("graph {}={}".format(self.site_id, dict(graph)))
        return graph
