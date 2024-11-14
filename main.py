from neo4j import GraphDatabase, basic_auth
from datetime import datetime

# Connect to the Neo4j database
driver = GraphDatabase.driver(
    "url:port",
    auth=basic_auth("username", "password")
)

# Function to add a customer
def add_customer(name, address, nic, phone, gender, dob):
    cypher_query = '''
    MERGE (c:Customer {name: $name, address: $address, nic: $nic, phone: $phone, gender: $gender, dateOfBirth: $dob})
    RETURN c
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, name=name, address=address, nic=nic, phone=phone, gender=gender, dob=dob)
        )
        print(f"Customer {name} added to the database.")

# Function to add a spa
def add_spa(spa_id, location, registration_number):
    cypher_query = '''
    MERGE (s:Spa {spa_id: $spa_id, location: $location, registration_number: $registration_number})
    RETURN s
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, spa_id=spa_id, location=location, registration_number=registration_number)
        )
        print(f"Spa {location} added to the database.")

# Function to add a therapist
def add_therapist(therapist_id, name, address, gender, nic):
    cypher_query = '''
    MERGE (t:Therapist {therapist_id: $therapist_id, name: $name, address: $address, gender: $gender, nic: $nic})
    RETURN t
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, therapist_id=therapist_id, name=name, address=address, gender=gender, nic=nic)
        )
        print(f"Therapist {name} added to the database.")

# Function to assign therapist to a spa (with start and end time)
def assign_therapist_to_spa(therapist_id, spa_id, start_time, end_time=None, current_spa=False):
    # End the previous relationship (if exists) by setting end_time
    cypher_query = '''
    MATCH (t:Therapist {therapist_id: $therapist_id})
    MATCH (s:Spa {spa_id: $spa_id})
    OPTIONAL MATCH (t)-[r:WORKS_AT]->(s)
    WHERE r.current_spa = true
    SET r.end_time = $end_time, r.current_spa = false
    '''  
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, therapist_id=therapist_id, spa_id=spa_id, end_time=end_time)
        )
        print(f"Previous relationship ended for therapist {therapist_id} at spa {spa_id}.")

    # Create a new relationship with the new spa
    cypher_query = '''
    MATCH (t:Therapist {therapist_id: $therapist_id})
    MATCH (s:Spa {spa_id: $spa_id})
    MERGE (t)-[r:WORKS_AT]->(s)
    SET r.start_time = $start_time, r.end_time = $end_time, r.current_spa = $current_spa
    RETURN t, s, r
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, therapist_id=therapist_id, spa_id=spa_id, start_time=start_time, end_time=end_time, current_spa=current_spa)
        )
        print(f"Therapist {therapist_id} assigned to spa {spa_id} with start time {start_time}.")

# Function to record a visit by a customer to a spa
def add_visit(customer_nic, spa_id, visit_date):
    cypher_query = '''
    MATCH (c:Customer {nic: $nic})
    MATCH (s:Spa {spa_id: $spa_id})
    MERGE (c)-[:VISITS {date: $visit_date}]->(s)
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, nic=customer_nic, spa_id=spa_id, visit_date=visit_date)
        )
        print(f"Visit recorded for customer {customer_nic} to spa {spa_id} on {visit_date}.")

# Function to record a treatment by a therapist for a customer
def add_treatment(customer_nic, therapist_id, treatment_date):
    cypher_query = '''
    MATCH (c:Customer {nic: $nic})
    MATCH (t:Therapist {therapist_id: $therapist_id})
    MERGE (c)-[:TREATED_BY {date: $treatment_date}]->(t)
    '''
    with driver.session(database="neo4j") as session:
        session.execute_write(
            lambda tx: tx.run(cypher_query, nic=customer_nic, therapist_id=therapist_id, treatment_date=treatment_date)
        )
        print(f"Treatment recorded for customer {customer_nic} with therapist {therapist_id} on {treatment_date}.")

# Example usage
add_customer("Alice Smith", "123 Main St", "123456789V", "555-1234", "Female", "1990-05-15")
add_spa("S001", "Downtown Spa", "REG12345")
add_spa("S002", "Uptown Spa", "REG54321")
add_therapist("T001", "John Doe", "456 Oak St", "Male", "T123456789")

# Assign therapist to spa (first assignment)
start_time = "2024-11-15T09:00:00"
assign_therapist_to_spa("T001", "S001", start_time, current_spa=True)

# Customer visits the spa
add_visit("123456789V", "S001", "2024-11-15")

# Therapist switches spa
end_time = "2024-11-20T17:00:00"  # End time for previous spa
start_time = "2024-11-21T09:00:00"  # Start time for new spa
assign_therapist_to_spa("T001", "S002", start_time, end_time=end_time, current_spa=True)

# Customer is treated by therapist
add_treatment("123456789V", "T001", "2024-11-15T10:00:00")

# Close the driver
driver.close()
