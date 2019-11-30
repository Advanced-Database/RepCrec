from data_manager import DataManager
import re


class TransactionManager:
    def __init__(self):
        # print("Init Transaction Manager!")
        self.data_manager_nodes = []
        for site_id in range(1, 11):
            self.data_manager_nodes.append(DataManager(site_id))

    def parse_instruction(self, line):
        instr_line = re.findall(r"[\w']+", line)
        return {
            'begin': self.begin,
            'beginRO': self.beginro,
            'R': self.read,
            'W': self.write,
            'dump': self.dump,
            'end': self.end,
            'fail': self.end,
            'recover': self.end,
        }.get(instr_line[0], self.invalid_instr)(instr_line)

    # -----------------------------------------------------
    # -------------- Instruction Executions ---------------
    # -----------------------------------------------------
    def begin(self, instr_line):
        transaction_id = instr_line[1]
        print(transaction_id + " begins.")

    def beginro(self, instr_line):
        transaction_id = instr_line[1]
        print(transaction_id + " begins and is read-only.")

    def read(self, instr_line):
        transaction_id = instr_line[1]
        variable = instr_line[2]
        print(transaction_id + " read " + variable + ".")

    def write(self, instr_line):
        transaction_id = instr_line[1]
        variable = instr_line[2]
        value = instr_line[3]
        print(transaction_id + " write " + variable +
              " with value " + value + ".")

    def dump(self, instr_line):
        print("Dump all data at all sites!")

    def end(self, instr_line):
        transaction_id = instr_line[1]
        print(transaction_id + " ends (commits or aborts).")

    def fail(self, instr_line):
        site_id = instr_line[1]
        print("Site " + site_id + " fails.")

    def recover(self, instr_line):
        site_id = instr_line[1]
        print("Site " + site_id + " recovers.")

    def invalid_instr(self, instr_line):
        print("Invalid instruction: " + " ".join(instr_line))

    # -----------------------------------------------------
    # -----------------------------------------------------
    # -----------------------------------------------------

    def monitor_site_status(self):
        for dm in self.data_manager_nodes:
            if dm.is_up:
                pass
            else:
                pass

    def output_site_status(self):
        print("-------------------- Dump all the output --------------------")
        for dm in self.data_manager_nodes:
            if dm.is_up:
                print("Site" + str(dm.id) + " is up")
            else:
                print("Site" + str(dm.id) + "'s status: Down")

            dm_info = dm.dump(dm.id)
            print("Site" + str(dm.id) + "'s data: " + dm_info)
