#include <iostream>
#include <pqxx/pqxx>

#include "exerciser.h"

using namespace std;
using namespace pqxx;

// prototypes
void createRelations(pqxx::connection & conn);
void fillTables(pqxx::connection & conn);

int main (int argc, char *argv[]) {

  //Allocate & initialize a Postgres connection object
  connection *C;

  try{
    //Establish a connection to the database
    //Parameters: database name, user name, user password
    C = new connection("dbname=ACC_BBALL user=postgres password=passw0rd");
    if (C->is_open()) {
      cout << "Opened database successfully: " << C->dbname() << endl;
    } else {
      cout << "Can't open database" << endl;
      return 1;
    }
  } catch (const std::exception &e){
    cerr << e.what() << std::endl;
    return 1;
  }


  //TODO: create PLAYER, TEAM, STATE, and COLOR tables in the ACC_BBALL database
  //      load each table with rows from the provided source txt files


  exercise(C);


  //Close database connection
  C->disconnect();

  return 0;
}

/**
 *
 * @param conn connection with db
 */
void createRelations(pqxx::connection & conn) {
    pqxx::work txn(conn);
    stringstream ss;
    ss << "create table STATE ("
       << "STATE_ID integer primary key,"
       << "NAME varchar(30) not null )";
    txn.exec(ss.str());

    stringstream ssc;
    ssc << "create table COLOR ("
        << "COLOR_ID integer primary key,"
        << "NAME varchar(30) not null )";
    txn.exec(ssc.str());

    stringstream sst;
    sst << "create table TEAM ("
        << "TEAM_ID integer primary key,"
        << "NAME varchar(30) not null,"
        << "STATE_ID integer references STATE not null,"
        << "COLOR_ID integer references COLOR not null,"
        << "WINS integer not null,"
        << "LOSES integer not null )";
    txn.exec(sst.str());

    stringstream ssp;
    ssp << "create table PLAYER ("
        << "PLAYER_ID integer primary key,"
        << "TEAM_ID integer references TEAM not null,"
        << "UNIFORM_NUM integer not null,"
        << "FIRST_NAME varchar(40) not null,"
        << "LAST_NAME varchar(40) not null,"
        << "MPG integer not null,"
        << "PPG integer not null,"
        << "RPG integer not null,"
        << "APG integer not null,"
        << "SPG integer not null,"
        << "BPG integer not null )";
    txn.exec(ssp.str());
    txn.commit();
}

/**
 *
 * @param conn connection with db
 */
void fillTables(pqxx::connection & conn) {

}
