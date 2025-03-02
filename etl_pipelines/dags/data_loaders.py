from abc import ABC, abstractmethod
import psycopg2 
import psycopg2.extras as extras

class DataLoader(ABC):
    @abstractmethod
    def load_data(self, df, target_schema, target_table, db, user, password, host, port):
        pass

class PostgresLoader(DataLoader):
    def load_data(self, df, target_schema, target_table, db, user, password, host, port):
        # establishing connection 
        conn = psycopg2.connect( 
            database=db, 
            user=user, 
            password=password, 
            host=host, 
            port=port
        ) 
        
        tuples = [tuple(x) for x in df.to_numpy()] 
    
        cols = ','.join(list(df.columns)) 
    
        # SQL query to execute 
        query = "INSERT INTO %s.%s(%s) VALUES %%s" % (target_schema, target_table, cols) 
        cursor = conn.cursor() 
        try: 
            extras.execute_values(cursor, query, tuples) 
            conn.commit() 
        except (Exception, psycopg2.DatabaseError) as error: 
            print("Error: %s" % error) 
            conn.rollback() 
            cursor.close() 
            return 1
        cursor.close() 
  