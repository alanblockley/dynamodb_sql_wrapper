import mysql_to_ddb_class as mysql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

wrapper = mysql.DynamoDBSQLWrapper()

def fIsDeathCard(card: str) -> bool:
    card = card.lower()
    sql = f"SELECT card_death FROM compendium_cards WHERE card_shortname = '{card}'"
    logger.info(f"Executing query: {sql}")
    result = wrapper.execute_query(sql)
    
    logger.info(f"Result for card '{card}': {result}")
    return result

def fTransact(status: str, action: str, userkey: str, username: str, location: str, pos: str, srcinfo: str = "") -> bool:
    sql = f"""
    INSERT INTO compendium_transactions VALUES (NULL,NOW(),{status},'{action}','{userkey}','{username}','{location}','{pos}','{objectName}: {srcinfo}')
    """
    logger.debug(f"Executing transaction query: {sql}")
    result = wrapper.execute_query(sql)
    logger.info(f"Transaction result: {result}")
    return result

def fGetWords() -> list[str]:
    sql = "SELECT word FROM compendium_questwords"
    logger.info(f"Executing query: {sql}")
    result = wrapper.execute_query(sql)
    logger.info(f"Word result: {result}")
    return result

if __name__ == "__main__":
    logger.info("Starting main execution")
    
    # # Example usage
    # 
    # sql_query = """
    # SELECT id, name, age
    # FROM users
    # WHERE age > 18
    # ORDER BY name ASC
    # LIMIT 10;
    # """

    # result = wrapper.execute_query(sql_query)
    # print("DynamoDB Query Parameters:")
    # print(result)


    # sql_query = """
    # INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);
    # """

    # result = wrapper.execute_query(sql_query)
    # print("DynamoDB Query Parameters:")
    # print(result)


    result = fIsDeathCard("Death")
    logger.info(f"Final result: {result}")

    words = fGetWords()
    logger.info(f"Words: {words}")
    for word in words:
        print(word)