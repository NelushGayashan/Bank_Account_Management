class BankAccountManager:
    def __init__(self, file_name):
        # Initialize the BankAccountManager object with the file name and create the file if it doesn't exist.
        self.file_name = file_name
        self._create_file_if_not_exist()

    def _create_file_if_not_exist(self):
        # Create the account file if it does not exist.
        try:
            with open(self.file_name, 'x'):
                pass  # File created successfully
        except FileExistsError:
            pass  # File already exists

    def get_balance(self, account_number):
        # Retrieve the balance of a given account number.
        account_info = self._read_accounts()
        if account_number in account_info:
            return account_info[account_number]
        else:
            print(f"Account {account_number} not found.")
            return None

    def deposit(self, account_number, amount):
        # Deposit an amount into a specified account number.
        account_info = self._read_accounts()
        if account_number in account_info:
            current_balance = account_info[account_number]
            new_balance = current_balance + amount
            account_info[account_number] = new_balance
            # Update the account with deposit details
            self._update_account(account_number, new_balance, f"deposit -> {amount}", f"Total -> {new_balance}")
            print(f"Deposited {amount} into account {account_number}. New balance: {new_balance}")
        else:
            print(f"Account {account_number} not found.")

    def withdraw(self, account_number, amount):
        # Withdraw an amount from a specified account number.
        account_info = self._read_accounts()
        if account_number in account_info:
            current_balance = account_info[account_number]
            if current_balance >= amount:
                new_balance = current_balance - amount
                account_info[account_number] = new_balance
                # Update the account with withdrawal details
                self._update_account(account_number, new_balance, f"withdraw -> {amount}", f"Total -> {new_balance}")
                print(f"Withdrew {amount} from account {account_number}. New balance: {new_balance}")
            else:
                print(f"Insufficient balance in account {account_number}.")
        else:
            print(f"Account {account_number} not found.")

    def update_account(self, account_number, balance):
        # Update the balance of a specified account number.
        self._update_account(account_number, balance, "update", f"Total -> {balance}")

    def _read_accounts(self):
        # Read account information from the file and return as a dictionary.
        account_info = {}
        try:
            with open(self.file_name, 'r') as file:
                for line in file:
                    if line.strip():  # Ensure the line is not empty
                        parts = line.strip().split(',')
                        if len(parts) >= 2:
                            account_number = parts[0]
                            balance = float(parts[1])
                            account_info[account_number] = balance
        except FileNotFoundError:
            print(f"Error: File '{self.file_name}' not found.")
        except IOError:
            print(f"Error: Could not read from file '{self.file_name}'.")
        return account_info

    def _update_account(self, account_number, balance, action, total_message):
        # Append updated account information to the file.
        try:
            with open(self.file_name, 'a') as file:
                file.write(f"{account_number},{balance},{action},{total_message}\n")
        except IOError:
            print(f"Error: Could not write to file '{self.file_name}'.")

# Example usage of BankAccountManager class
file_name = 'accounts.txt'
bank_manager = BankAccountManager(file_name)

# Test operations on account 'A123'
print("Balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.deposit('A123', 500.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.withdraw('A123', 200.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.update_account('A123', 1500.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))
