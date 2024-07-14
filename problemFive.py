import psycopg2 # type: ignore
from psycopg2 import Error # type: ignore

class BankAccountManager:
    def __init__(self, db_host, db_name, db_user, db_password):
        try:
            self.connection = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password
            )
            self.cursor = self.connection.cursor()
            self._create_accounts_table_if_not_exist()
            self._create_transactions_table_if_not_exist()
        except Error as e:
            print(f"Error while connecting to PostgreSQL: {e}")

    def _create_accounts_table_if_not_exist(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS accounts (
                account_number VARCHAR(20) PRIMARY KEY,
                balance NUMERIC
            )
        '''
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except Error as e:
            print(f"Error while creating accounts table: {e}")

    def _create_transactions_table_if_not_exist(self):
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                account_number VARCHAR(20),
                transaction_type VARCHAR(20),
                amount NUMERIC,
                new_balance NUMERIC,
                transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_number) REFERENCES accounts(account_number)
            )
        '''
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except Error as e:
            print(f"Error while creating transactions table: {e}")

    def get_balance(self, account_number):
        try:
            self.cursor.execute("SELECT balance FROM accounts WHERE account_number = %s", (account_number,))
            balance = self.cursor.fetchone()
            if balance:
                return balance[0]
            else:
                print(f"Account {account_number} not found.")
                return None
        except Error as e:
            print(f"Error while retrieving balance: {e}")
            return None

    def deposit(self, account_number, amount):
        try:
            current_balance = self.get_balance(account_number)
            if current_balance is not None:
                new_balance = current_balance + amount
                self.cursor.execute("UPDATE accounts SET balance = %s WHERE account_number = %s", (new_balance, account_number))
                self.connection.commit()
                self._log_transaction(account_number, "deposit", amount, new_balance)
                print(f"Deposited {amount} into account {account_number}. New balance: {new_balance}")
        except Error as e:
            print(f"Error while depositing amount: {e}")

    def withdraw(self, account_number, amount):
        try:
            current_balance = self.get_balance(account_number)
            if current_balance is not None and current_balance >= amount:
                new_balance = current_balance - amount
                self.cursor.execute("UPDATE accounts SET balance = %s WHERE account_number = %s", (new_balance, account_number))
                self.connection.commit()
                self._log_transaction(account_number, "withdraw", amount, new_balance)
                print(f"Withdrew {amount} from account {account_number}. New balance: {new_balance}")
            elif current_balance is not None:
                print(f"Insufficient balance in account {account_number}.")
        except Error as e:
            print(f"Error while withdrawing amount: {e}")

    def update_account(self, account_number, balance):
        try:
            self.cursor.execute("UPDATE accounts SET balance = %s WHERE account_number = %s", (balance, account_number))
            self.connection.commit()
            print(f"Updated balance of account {account_number}: {balance}")
        except Error as e:
            print(f"Error while updating account: {e}")

    def _log_transaction(self, account_number, transaction_type, amount, new_balance):
        try:
            insert_query = '''
                INSERT INTO transactions (account_number, transaction_type, amount, new_balance)
                VALUES (%s, %s, %s, %s)
            '''
            self.cursor.execute(insert_query, (account_number, transaction_type, amount, new_balance))
            self.connection.commit()
        except Error as e:
            print(f"Error while logging transaction: {e}")

    def __del__(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed")

# Example usage of BankAccountManager class
db_host = 'localhost'
db_name = 'bank_accounts'
db_user = 'your_username'
db_password = 'your_password'

bank_manager = BankAccountManager(db_host, db_name, db_user, db_password)

# Test operations on account 'A123'
print("Balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.deposit('A123', 500.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.withdraw('A123', 200.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))

bank_manager.update_account('A123', 1500.0)
print("Updated balance of account 'A123':", bank_manager.get_balance('A123'))
