from abc import ABC, abstractmethod
import psycopg2 
import psycopg2.extras as extras

class DataLoader(ABC):
    @abstractmethod
    def load_data(self, df, target_schema, target_table, db, user, password, host, port, conflict_strategy):
        pass

class PostgresLoader(DataLoader):
    def load_data(self, df, target_schema, target_table, db, user, password, host, port, conflict_strategy="ignore"):
        conn = psycopg2.connect(
            database=db, 
            user=user, 
            password=password, 
            host=host, 
            port=port
        )
        
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))

        if conflict_strategy == "ignore":
            conflict_clause = "ON CONFLICT DO NOTHING"
        elif conflict_strategy == "update":
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != "id"])
            conflict_clause = f"ON CONFLICT (id) DO UPDATE SET {update_set}"
        else:
            conflict_clause = ""

        query = f"""
            INSERT INTO {target_schema}.{target_table} ({cols}) 
            VALUES %s {conflict_clause}
        """

        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            raise error
        cursor.close()
  