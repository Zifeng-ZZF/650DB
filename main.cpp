#include <iostream>
#include <fstream>
#include <pqxx/pqxx>
#include <algorithm>
#include <string>
#include <sstream>
#include <unordered_map>

#include "exerciser.h"

using namespace std;
using namespace pqxx;

// prototypes
void createRelations(pqxx::connection & conn);
void fillTables(pqxx::connection & conn, unordered_map<string, string> & tbs);
void droppingAll(connection & conn);
void filesChecker(unordered_map<string, string> & tbs);
void parsePlayer(const string & name, connection & C);
void parseTeam(const string & name, connection & C);
void parseState(const string & name, connection & C);
void parseColor(const string & name, connection & C);

/**
 * Main entry
 * @param argc num of arg
 * @param argv array of args
 */
int main (int argc, char *argv[]) {
    unordered_map<string, string> tbs;
    tbs["COLOR"] = "color.txt";
    tbs["STATE"] = "state.txt";
    tbs["TEAM"] = "team.txt";
    tbs["PLAYER"] = "player.txt";
    filesChecker(tbs);

    //Allocate & initialize a Postgres connection object
    connection *C;

    try{
        //Establish a connection to the database
        //Parameters: database name, user name, user password
        C = new connection("dbname=ACC_BBALL user=postgres password=passw0rd");
        cout << "Try connecting to " << C->hostname() << " at port " << C->port() << endl;
        if (C->is_open()) {
            cout << "Opened database successfully: " << C->dbname() << endl;
        } 
        else {
            cout << "Can't open database" << endl;
            return 1;
        }
    } 
    catch (const std::exception &e) {
        cerr << e.what() << std::endl;
        return 1;
    }

    //TODO: create PLAYER, TEAM, STATE, and COLOR tables in the ACC_BBALL database
    //      load each table with rows from the provided source txt files

    droppingAll(*C);
    createRelations(*C);
    fillTables(*C, tbs);

    exercise(C);

    // Close database connection
    C->disconnect();
    return 0;
}


/**
 * check whether necessary files exist in the working directory
 * @param tbs map of DB table name and input file name
 */
void filesChecker(unordered_map<string, string> & tbs) {
    for (auto & item : tbs) {
        ifstream ifs(item.second);
        if (!ifs.good()) {
            cerr << item.second << " not exist." << endl;
            exit(1);
        }
        ifs.close();
    }
}


/**
 * drop existing tables if there is any
 * @param conn connection
 */
void droppingAll(connection & conn) {
    try {
        work txn(conn);
        stringstream dropSql;
        dropSql << "drop table \"PLAYER\";"
                << "drop table \"TEAM\";"
                << "drop table \"STATE\";"
                << "drop table \"COLOR\";";
        txn.exec(dropSql.str());
        txn.commit();
    }
    catch (pqxx::undefined_table & e) {
        cout << "table not exist, go on without dropping." << endl;
        return;
    } 
}

/**
 * Create tables on start, including execute sqls in a transaction
 * @param conn connection with db
 */
void createRelations(pqxx::connection & conn) {
    pqxx::work txn(conn);
    stringstream ss;
    ss << "create table \"STATE\" ("
       << "\"STATE_ID\" serial primary key,"
       << "\"NAME\" varchar(30) not null )";
    txn.exec(ss.str());

    stringstream ssc;
    ssc << "create table \"COLOR\" ("
        << "\"COLOR_ID\" serial primary key,"
        << "\"NAME\" varchar(30) not null )";
    txn.exec(ssc.str());

    stringstream sst;
    sst << "create table \"TEAM\" ("
        << "\"TEAM_ID\" serial primary key,"
        << "\"NAME\" varchar(30) not null,"
        << "\"STATE_ID\" integer references \"STATE\" not null,"
        << "\"COLOR_ID\" integer references \"COLOR\" not null,"
        << "\"WINS\" integer not null,"
        << "\"LOSSES\" integer not null )";
    txn.exec(sst.str());

    stringstream ssp;
    ssp << "create table \"PLAYER\" ("
        << "\"PLAYER_ID\" serial primary key,"
        << "\"TEAM_ID\" integer references \"TEAM\" not null,"
        << "\"UNIFORM_NUM\" integer not null,"
        << "\"FIRST_NAME\" varchar(40) not null,"
        << "\"LAST_NAME\" varchar(40) not null,"
        << "\"MPG\" integer not null,"
        << "\"PPG\" integer not null,"
        << "\"RPG\" integer not null,"
        << "\"APG\" integer not null,"
        << "\"SPG\" double precision not null,"
        << "\"BPG\" double precision not null )";
    txn.exec(ssp.str());
    txn.commit();
}

/**
 * fill table specified by name with data from files also specified by name
 * @param conn connection with db
 * @param name name of src file
 */
void fillTables(pqxx::connection & conn, unordered_map<string, string> & tbs) {
    parseColor(tbs["COLOR"], conn);
    parseState(tbs["STATE"], conn);
    parseTeam(tbs["TEAM"], conn);
    parsePlayer(tbs["PLAYER"], conn);
}


/**
 * parse player data input files and connect to DB then inserting the rows into DB
 * @param name name of the input file
 * @param C connection object
 */
void parsePlayer(const string & name, connection & C) {
    ifstream input(name);
    if (input.is_open()) {
        string line;
        while (getline(input, line)) {
            stringstream liness(line);
            int player_id;
            int team_id, jersey_num;
            string first_name, last_name;
		    int mpg, ppg, rpg, apg;
            double spg, bpg;
            liness >> player_id >> team_id >> jersey_num >> first_name >> last_name 
                   >> mpg >> ppg >> rpg >> apg >> spg >> bpg;
            // cout << "Inserting row: \n";
            // cout << "(" << player_id << "," << team_id << "," << jersey_num << "," << first_name << "," << last_name 
            //      << mpg << "," << ppg << "," << rpg << "," << apg << "," << spg << "," << bpg << ")" << endl;
            add_player(&C, team_id, jersey_num, first_name, last_name, mpg, ppg, rpg, apg, spg, bpg);
        }
    }
}


/**
 * parse team data input files and connect to DB then inserting the rows into DB
 * @param name name of the input file
 * @param C connection object
 */
void parseTeam(const string & name, connection & C) {
    ifstream input(name);
    if (input.is_open()) {
        string line;
        while (getline(input, line)) {
            stringstream liness(line);
            int team_id;
            string name;
            int state_id, color_id, wins, losses;
            liness >> team_id >> name >> state_id >> color_id >> wins >> losses; 
            add_team(&C, name, state_id, color_id, wins, losses);
        }
    }
}


/**
 * parse state data input files and connect to DB then inserting the rows into DB
 * @param name name of the input file
 * @param C connection object
 */
void parseState(const string & name, connection & C) {
    ifstream input(name);
    if (input.is_open()) {
        string line;
        while (getline(input, line)) {
            stringstream liness(line);
            int state_id;
            string name;
            liness >> state_id >> name;
            add_state(&C, name);
        }
    }
}


/**
 * parse color data input files and connect to DB then inserting the rows into DB
 * @param name name of the input file
 * @param C connection object
 */
void parseColor(const string & name, connection & C) {
    ifstream input(name);
    if (input.is_open()) {
        string line;
        while (getline(input, line)) {
            stringstream liness(line);
            int color_id;
            string name;
            liness >> color_id >> name;
            add_color(&C, name);
        }
    }
}
