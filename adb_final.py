import time
import sys

# sites = database


class Database:
    databases = {}  # {site_id: Database}
    all_sites = []  # [Database, Database....]
    # a site = a datebase

    def __init__(self, site_id):
        """
        Name:
            __init__
        Args:
            site_id
        """
        self.site_id = site_id  # Integer
        self.status = "up"  # default two status: up and down
        self.fail_time = None
        self.init_time = time.time()
        # {variable: [{value:, time:, trans_id:,}, {}, ...], var2: [{},{}], ...}
        self.uncommitted_records = {}
        # {variable: [{value:, time:, trans_id:, writeonly: }, {value:, time:, trans_id:}, ...] ,variable2: ,...}commited history of each variable
        self.committed_records = {}

    @classmethod
    def initialize_sites(cls, num_sites, num_variables):
        """
        Name:
            initialize_sites
        Args:
            num_sites
            num_variables
        """
        cls.databases = {site_id: Database(site_id)
                         for site_id in range(1, num_sites + 1)}
        cls.all_sites = list(cls.databases.values())

        for i in range(1, num_variables+1):
            site_id = 1 + (i % 10)
            value = 10 * i
            variable = i
            curr_tm = time.time()
            if i % 2 == 1:  # variables with odd index
                cls.databases[site_id].initialize_variable(
                    variable, value, curr_tm)
            else:  # variables with even index
                for database in cls.all_sites:
                    database.initialize_variable(variable, value, curr_tm)

    def initialize_variable(self, variable, value, curr_tm):
        """
        Name:
            initialize_variable
        Args:
            variable
            value
            tm
        """
        if variable not in self.committed_records:
            self.committed_records[variable] = []
        self.committed_records[variable].append(
            {"value": value, "time": curr_tm, "trans_id": 0, "writeonly": 0})

    # check if the version of variable is unique in all up sites
    def check_if_unique(self, variable, var_time):
        """
        Name:
            check_if_unique
        Args:
            variable
            var_time
        Returns:
            (type)bool: Return False means the version of variable is not unique, otherwise return True
        """
        repeat_num = 0
        for site in self.all_sites:
            if site.status == "up":
                variable_record_list = site.committed_records.get(variable)
                if variable_record_list is not None:
                    for record in variable_record_list:
                        if record["time"] == var_time:
                            repeat_num += 1
                            break
        if repeat_num >= 2:
            return False
        else:
            return True

    # interate the data of the site
    def dump_site_data(self):
        """
        Name:
            dump_site_data
        Output:
            print the data of the site
        """
        print(f"site {self.site_id} - ", end="")
        for index, (variable, rec_list) in enumerate(self.committed_records.items()):
            print(f"x{variable}: {rec_list[-1]['value']}", end="")
            # check whether the index is the last one or not
            if index < len(self.committed_records) - 1:
                print(", ", end="")
        print()

    # set status = down and record fail time
    def down(self):
        """
        Name:
            down
        Returns:
            (type)int: return the fail time of the site
        """
        if self.status == "up":
            self.status = "down"
            self.fail_time = time.time()
            self.uncommitted_records = {}
            if TM.down_history.get(self.site_id) is not None:
                TM.down_history[self.site_id].append(self.fail_time)
            else:
                down_time_list = [self.fail_time]
                TM.down_history[self.site_id] = down_time_list
            return self.fail_time

    # set status = up, recover data from master site, record the recover time
    # for every commited version of variable, check whether it is unique,
    # if it isn't unique, set the writeonly attribute to 1
    def up(self):
        """
        Name:
            up
        """
        if self.status == "down":
            self.status = "up"
            for variable, records in self.committed_records.items():
                if variable % 2 == 0:
                    for version in records:
                        # this version is not unique
                        if self.check_if_unique(variable, version["time"]) is False:
                            version["writeonly"] = 1

    def read(self, variable, cmm_time):
        """
        Name:
            read
        Args:
            variable
            cmm_time
        Returns:
            (type)int: return the committed value of the version of variable
        """
        # Check if the site is up
        cm_list = self.committed_records[variable]

        if self.status == "up":
            for i in range(len(cm_list)-1, -1, -1):
                if cmm_time == cm_list[i]["time"]:
                    if cm_list[i]["writeonly"] == 0:  # writeonly == 1 : cannot read
                        committed_val = cm_list[i]["value"]
                        return committed_val
                    else:
                        return -1
            return -1
        else:
            return -1

    def write(self, trans_id, variable, new_value):
        """
        Name:
            write
        Args:
            trans_id
            variable
            new_value
        Returns:
            (type)int: return -1 means the write operation failed
        """
        # Check if the site is up
        if self.status == "up":
            # Check if the site possesses the variable x or the copy of x
            new_uncommitted = {"value": new_value,
                               "time": time.time(), "trans_id": trans_id}
            if variable in self.uncommitted_records:
                self.uncommitted_records[variable].append(new_uncommitted)
            else:
                self.uncommitted_records[variable] = [new_uncommitted]
        else:
            return -1

    # commit(Ti)
    # Assume passed transaction check can commit
    def commit(self, trans_id, variable, to_commit_time):
        """
        Name:
            commit
        Args:
            trans_id
            variable
            to_commit_time
        Returns:
            (type)bool: return True means the commit success, otherwise return False
        """
        flag = 0
        if self.status == "up":
            if not self.uncommitted_records:
                return False
            records_to_remove = []
            if variable not in self.uncommitted_records:
                return False
            uncomm_list = self.uncommitted_records[variable]
            # iterate every dic in uncomm_list
            for i, uncomm_record in enumerate(uncomm_list):
                if uncomm_record.get("trans_id", 0) == trans_id:
                    new_committed_record = {
                        "value": uncomm_record["value"], "time": to_commit_time, "trans_id": uncomm_record["trans_id"], "writeonly": 0}
                    self.committed_records[variable].append(
                        new_committed_record)
                    # mark index to remove in uncomm_list[]
                    records_to_remove.append(i)
                    flag = 1
            # remove records
            for i in reversed(records_to_remove):
                uncomm_list.pop(i)
            return flag == 1
        else:
            return False


class Transaction:
    def __init__(self, trans_id):
        self.trans_id = trans_id
        # "active", "waiting", "uncommitted", "committed", "aborted", or None
        self.status = "active"
        self.create_time = time.time()  # transaction create time
        self.committed_time = None
        # {trans_id:1, site_id: 4, type:W, variable: 3, value: 88, op_time: time.time()}
        self.ops = []
        # [{"type":"R", "variable":3, "trans_id":1},# {"type": "W", "variable": 4, "trans_id": 1, "value": 90},...,]
        self.waiting_ops = []
        self.dependencies = set()


class TM:
    trans_list = {}  # {trans_id: trans, ...} class-level variable to store all transactions
    sites_dic = {}  # {site_id: site1, site_id2: site2 ...} class-level variable to store all copied sites data
    # [{tran_id:1, site_id: 4, type:W, variable: 3, value: 88, op_time: time.time()}, {}, {}]
    operation_list = []
    committed_trans = []  # [{trans_id: , committed_time: }, ...]
    down_history = {}  # {site_id: [time.time(), ..]}
    var_committed_history = {}  # {variable: [{time: , site_ids:[] }, ... ], }
    waiting_trans_dic = {}  # {site_id: [trans_id1,trans_id2], ... ,}

    def __init__(self, all_sites):
        """
        Name:
            __init__
        Args:
            all_sites
        """
        TM.sites_dic = all_sites  # update class-level sites_dic
        for site_id, site in TM.sites_dic.items():
            for var, rec_list in site.committed_records.items():
                dic = {"time": rec_list[-1]["time"], "site_ids": [site_id]}
                if var in TM.var_committed_history:
                    TM.var_committed_history[var][0]["site_ids"].append(
                        site_id)
                else:
                    TM.var_committed_history[var] = [dic]

    # create a new transaction
    def create_trans(self, trans_id):
        """
        Name:
            create_trans
        Args:
            trans_id
        Returns:
            (type)instance: return a new Transaction instance
        """
        new_transaction = Transaction(trans_id)
        # add new trans into trans_list
        TM.trans_list[trans_id] = new_transaction
        return new_transaction

    def can_commit(self, trans_id, variable, site_id):
        """
        Name:
            can_commit
        Args:
            trans_id
            variable
            site_id
        Returns:
            (type)bool: return False means the transaction cannot commit on this site, otherwise return True 
        """
        site = TM.sites_dic[site_id]
        if site.status == "down" or site.uncommitted_records == {}:
            return False
        if variable not in site.uncommitted_records:
            return False
        if site.uncommitted_records[variable] == []:
            return False
        flag = False
        for record in site.uncommitted_records[variable]:
            if record["trans_id"] == trans_id:
                flag = True
        return flag

    def add_dependency(self, trans_id):
        """
        Name:
            add_dependency
        Args:
            trans_id
        """
        committed_tran_ids = list(entry["trans_id"]
                                  for entry in TM.committed_trans)
        curr_tran = TM.trans_list[trans_id]  # T3
        for committed_tran_id in committed_tran_ids:
            tran = TM.trans_list[committed_tran_id]  # T4
            tran_op_list = TM.trans_list[committed_tran_id].ops
            curr_tran_op_list = curr_tran.ops
            for op in tran_op_list:
                if op["type"] == "R":
                    read_var = op["variable"]
                    for curr_op in curr_tran_op_list:
                        if curr_op["variable"] == read_var:
                            if curr_op["type"] == "W":
                                tran.dependencies.add(trans_id)
                                break
                elif op["type"] == "W":
                    write_var = op["variable"]
                    for curr_op in curr_tran_op_list:
                        if curr_op["variable"] == write_var:
                            if curr_op["type"] == "R":
                                curr_tran.dependencies.add(committed_tran_id)
                            elif curr_op["type"] == "W":
                                curr_tran.dependencies.add(committed_tran_id)
                                tran.dependencies.add(trans_id)

    def detect_cycle(self):
        """
        Name:
            detect_cycle
        Args:
        Returns:
            (type)bool: return False means there is a r-w cycle, the transaction needs to be aborted
        """
        committed_tran_ids = list(entry["trans_id"]
                                  for entry in TM.committed_trans)
        for com_id in committed_tran_ids:
            if self.find_depend(com_id, set()) is False:
                return False

    def find_depend(self, trans_id, pass_set):
        """
        Name:
            find_depend
        Args:
            trans_id
            pass_set
        Returns:
            (type)bool: return False means there is a r-w cycle, otherwise return true
        """
        if trans_id in pass_set:
            return False
        pass_set.add(trans_id)
        # trans_id has dependency
        if len(TM.trans_list[trans_id].dependencies) > 0:
            for depend in TM.trans_list[trans_id].dependencies:
                if self.find_depend(depend, pass_set) is False:
                    return False
        else:
            return True

    # make a transaction wait and add the wait operaitons list
    def wait(self, trans_id, site_id, type_op, variable):
        """
        Name:
            wait
        Args:
            trans_id
            site_id
            type_op
            variable
        Output:
            print the transaction which is waiting
        """
        tran = TM.trans_list[trans_id]
        tran.status = "waiting"
        print(f"T{trans_id} is waiting site {site_id}")
        if site_id in TM.waiting_trans_dic:
            TM.waiting_trans_dic[site_id].append(trans_id)
        else:
            TM.waiting_trans_dic[site_id] = [trans_id]
        self.wait_ops_add(trans_id, type_op, variable, None)

    # add the waiting operaions list
    def wait_ops_add(self, trans_id, type_op, variable, new_value):
        """
        Name:
            wait_ops_add
        Args:
            trans_id
            type_op
            variable
            new_value
        """
        tran = TM.trans_list[trans_id]
        waiting_dic = {}
        waiting_dic["type"] = type_op
        waiting_dic["variable"] = variable
        waiting_dic["trans_id"] = trans_id
        waiting_dic["value"] = new_value
        tran.waiting_ops.append(waiting_dic)

    def remove_dependency(self, trans_id):
        """
        Name:
            remove_dependency
        Args:
            trans_id
        """
        for trans in TM.trans_list.values():
            if trans_id in trans.dependencies:
                trans.dependencies.remove(trans_id)
        TM.trans_list[trans_id].dependencies = set()

    # End(Ti)
    def end(self, trans_id):
        """
        Name:
            end
        Args:
            trans_id
        Output:
            print the committed transaction
        Side Effects:
            Firstly, if the transaction is waiting, make the transaction aborted.
            Then add the dependency to check the transaction has RW cycle, 
            if yes, then make it aborted. We also get records which there are other committed 
            transactions before this transaction. 
            Then we start iterate each operation in the transaction, 
            If the variable of a write operation has been committed in other committed transactions,
            make the transaction aborted. Then if any site can_commit is failed or commit is failed, 
            then the transaction aborted.
        """
        tran = TM.trans_list[trans_id]
        if tran.status == "waiting":
            self.abort(trans_id)
            return

        # rw cycle check
        self.add_dependency(trans_id)
        if self.detect_cycle() is False:
            self.abort(trans_id)
            self.remove_dependency(trans_id)
            return

        curr_time = time.time()
        begin_time = tran.create_time
        other_committed_trans_list = []
        for dic in TM.committed_trans:
            if dic["committed_time"] > begin_time and dic["committed_time"] < curr_time:
                other_committed_trans_list.append(
                    TM.trans_list[dic["trans_id"]])

        # get all ops[trans_id = trans_id]
        tran_ops = tran.ops
        # iterate all ops of trans_id
        for op in tran_ops:
            # if we have write op
            if op["type"] == "W":
                site_ids = op["site_id"]
                variable = op["variable"]
                for other_trans in other_committed_trans_list:
                    other_ops_list = other_trans.ops
                    for each_op in other_ops_list:
                        if each_op["type"] == "W" and each_op["variable"] == variable:
                            self.abort(trans_id)
                            return
                to_commit_time = time.time()  # send it to all sites

                # check all sites whether they can commit
                # if one can_commit failed then trans.status = aborted
                for site_id in site_ids:
                    if self.can_commit(trans_id, variable, site_id) is False:
                        self.abort(trans_id)
                        return
                # commit all sites affected
                for site_id in site_ids:
                    site_committed_res = TM.sites_dic[site_id].commit(
                        trans_id, variable, to_commit_time)
                    if site_committed_res is False:  # if any site.commit failed, we abort
                        self.abort(trans_id)
                        return

                # update variable_committed_history
                var_committed_history = {
                    "time": to_commit_time, "site_ids": site_ids}
                if TM.var_committed_history[variable] is not None:
                    TM.var_committed_history[variable].append(
                        var_committed_history)
                else:
                    TM.var_committed_history[variable] = [
                        var_committed_history]
        # update tran.committed time and add a trans committed history
        tran.committed_time = time.time()
        TM.committed_trans.append(
            {"trans_id": trans_id, "committed_time": tran.committed_time})
        print(f"T{TM.committed_trans[-1]['trans_id']} commits")

    def abort(self, trans_id):
        """
        Name:
            abort
        Args:
            trans_id
        Output:
            print the aborted transaction
        """
        TM.trans_list[trans_id].status = "aborted"
        print(f"T{trans_id} aborts")

    # R(Ti, xi)
    def read(self, trans_id, variable):
        """
        Name:
            read
        Args:
            trans_id
            variable
        Output:
            print the value of committed variable
        Side Effects:
            If the variable replicated in more than one site and cannot read 
            (all sites which have copies of the variable are failed and recovered), 
            we need to abort the transaction, 
            if the variable only in one site and cannot read (the site failed), 
            make transaction waiting.
        """
        tran = TM.trans_list[trans_id]  # get a transaction
        if tran.status == "waiting":
            self.wait_ops_add(trans_id, "R", variable, None)
            return

        tran_create_time = tran.create_time
        # to get the all committed history list of variable
        history_list = TM.var_committed_history.get(variable, [])
        site_list = []
        cmm_time = None

        for i in range(len(history_list)-1, -1, -1):
            # the committed time of variable
            cmm_time = history_list[i]["time"]
            if cmm_time < tran_create_time:
                site_list = history_list[i]["site_ids"]
                break

        committed_val = -1
        for site_id in site_list:
            site = TM.sites_dic[site_id]
            committed_val = site.read(variable, cmm_time)
            if committed_val != -1:
                print(f"T{trans_id} reads x{variable}: {committed_val}")
                dic = {"trans_id": trans_id, "site_id": site_id, "type": "R",
                       "variable": variable, "value": committed_val, "op_time": time.time()}
                tran.ops.append(dic)
                TM.operation_list.append(dic)
                break
        # if variable in more than one sites and cannot read
        if len(site_list) > 1:
            # check if all the sites in site_list failed
            flag_up = False
            for site_id in site_list:
                site = TM.sites_dic[site_id]
                if site.status == "up":
                    flag_up = True
                    break
            # At least one site is up and don't have the uncommited record, which means fail and recover, so aborts
            if flag_up is True and committed_val == -1:
                self.abort(trans_id)
            elif flag_up is False:
                # check the fail time of every site, find whether there exists one site that up until the transaction begins
                site_alive_nums = 0
                site_alive_list = []
                for site_id in site_list:
                    site_fail_time = TM.down_history[site_id][-1]
                    if site_fail_time > tran_create_time:
                        site_alive_nums += 1
                        site_alive_list.append(site_id)
                if site_alive_nums == 1:
                    self.wait(trans_id, site_alive_list[0], "R", variable)
                else:
                    self.abort(trans_id)
        # if the variable is at only one site and cannot read
        else:
            if committed_val == -1 and len(site_list) == 1:
                self.wait(trans_id, site_list[0], "R", variable)
            elif len(site_list) == 0:
                print(f"T{trans_id}, site = 0.")

    # W(Ti, xi, value)
    def write(self, trans_id, variable, new_value):
        """
        Name:
            write
        Args:
            trans_id
            variable
            new_value
        Output:
            print the sites affected by write operation
        """
        tran = TM.trans_list[trans_id]
        if tran.status == "waiting":
            self.wait_ops_add(trans_id, "W", variable, new_value)
            return
        wrote_sites = []
        if variable % 2 == 0:
            for site_id, site in TM.sites_dic.items():
                res = site.write(trans_id, variable, new_value)
                if res != -1:
                    wrote_sites.append(site_id)
            dic = {"trans_id": trans_id, "site_id": wrote_sites, "type": "W",
                   "variable": variable, "value": new_value, "op_time": time.time()}
            tran.ops.append(dic)
            TM.operation_list.append(dic)
            print(
                f"T{trans_id} wrote x{variable}: {new_value} at site {' '.join(map(str, wrote_sites))}")
        else:
            site_id = variable % 10 + 1
            res = TM.sites_dic[site_id].write(trans_id, variable, new_value)
            if res != -1:
                wrote_sites.append(site_id)
                dic = {"trans_id": trans_id, "site_id": wrote_sites, "type": "W",
                       "variable": variable, "value": new_value, "op_time": time.time()}
                tran.ops.append(dic)
                TM.operation_list.append(dic)
                print(
                    f"T{trans_id} wrote x{variable}: {new_value} at site {site_id}")

    # fail(site_id)
    def fail(self, site_id):
        """
        Name:
            fail
        Args:
            site_id
        """
        site = TM.sites_dic.get(site_id, None)
        if site.status == "up":
            fail_time = site.down()
            if fail_time is not None:
                if site_id in TM.down_history:
                    TM.down_history[site_id].append(fail_time)
                TM.down_history[site_id] = [fail_time]

    # recover(site_id)
    def recover(self, site_id):
        """
        Name:
            recover
        Args:
            site_id
        """
        site = TM.sites_dic[site_id]
        if site.status == "down":
            site.up()
        if site_id in TM.waiting_trans_dic:
            for trans_id in TM.waiting_trans_dic[site_id]:
                trans_instance = TM.trans_list[trans_id]
                self.awake(trans_instance)

    # Wake up a waiting transaction and make it keep working
    def awake(self, trans_instance):
        """
        Name:
            awake
        Args:
            trans_instance
        """
        trans_instance.status = "active"
        for operation in trans_instance.waiting_ops:
            if operation["type"] == "R":
                self.read(operation["trans_id"], operation["variable"])
            elif operation["type"] == "W":
                self.write(operation["trans_id"],
                           operation["variable"], operation["value"])

    def dump(self):
        """
        Name:
            dump
        Output:
            print all the data of all sites
        """
        print("=== output of dump ===")
        for site in TM.sites_dic.values():
            site.dump_site_data()

    def execute_test_case(self, file_path):
        """
        Name:
            execute_test_case
        Args:
            file_path
        """
        transactions = {}
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

            for line in lines:
                line = line.strip()

                if line.startswith("begin"):
                    trans_id = line.split("(")[1].split(")")[0][1]
                    transactions[trans_id] = self.create_trans(
                        int(trans_id.strip()))

                elif line.startswith("R"):
                    parts = line.split("(")[1].split(")")[0].split(",")
                    trans_id, variable_number = parts[0][1], parts[1].strip()[
                        1]
                    if variable_number is not None:
                        self.read(int(trans_id.strip()),
                                  int(variable_number.strip()))

                elif line.startswith("W"):
                    parts = line.split("(")[1].split(")")[0].split(",")
                    trans_id, variable_number, value = parts[0][1], parts[1].strip()[
                        1], parts[2]
                    if variable_number is not None:
                        self.write(int(trans_id.strip()), int(
                            variable_number.strip()), int(value.strip()))

                elif line.startswith("fail"):
                    site_id = int(line.split("(")[1].split(")")[0].strip())
                    self.fail(site_id)

                elif line.startswith("recover"):
                    site_id = int(line.split("(")[1].split(")")[0].strip())
                    self.recover(site_id)

                elif line.startswith("end"):
                    trans_id = line.split("(")[1].split(")")[0][1].strip()
                    if trans_id in transactions:
                        self.end(int(trans_id))

                elif line.startswith("dump"):
                    self.dump()

                else:
                    pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 adb_final.py input_file")
        sys.exit(1)

    input_file = sys.argv[1]

    # initialize we have 10 sites and 20 variables
    Database.initialize_sites(10, 20)
    tm = TM(Database.databases)  # Create an instance of TM
    tm.execute_test_case(input_file)  # Call the method on the instance
