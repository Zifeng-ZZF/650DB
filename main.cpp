#include <iostream>
#include <fstream>
#include <pqxx/pqxx>
#include <algorithm>
#include <string>
#include <sstream>

#include "exerciser.h"

using namespace std;
using namespace pqxx;

// prototypes
void createRelations(pqxx::connection & conn);
void fillTables(pqxx::connection & conn, const string & name);
void droppingAll(connection & conn);
void inputChecker(int argc, char *argv[]);

void parsePlayer(const string & name, connection & C);
void parseTeam(const string & name, connection & C);
void parseState(const string & name, connection & C);
void parseColor(const string & name, connection & C);

int main (int argc, char *argv[]) {

    inputChecker(argc, argv);

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
    fillTables(*C, argv[1]);
    fillTables(*C, argv[2]);
    fillTables(*C, argv[3]);
    fillTables(*C, argv[4]);

    // exercise(C);

    // Close database connection
    // C->disconnect();
    return 0;
}


void inputChecker(int argc, char *argv[]) {
    bool all_good = true;
    if (argc != 5) {
        cerr << "Wrong number of source files. Require 4." << endl;
        exit(1);
    }
    ifstream ifs1(argv[1]);
    ifstream ifs2(argv[2]);
    ifstream ifs3(argv[3]);
    ifstream ifs4(argv[4]);
    if (!ifs1.good()) {
        cerr << argv[1] << " not exist." << endl;
        all_good = false;
    }
    else if (!ifs2.good()) {
        cerr << argv[2] << " not exist." << endl;
        all_good = false;
    }
    else if (!ifs3.good()) {
        cerr << argv[3] << " not exist." << endl;
        all_good = false;
    }
    else if (!ifs4.good()) {
        cerr << argv[4] << " not exist." << endl;
        all_good = false;
    }
    ifs1.close();
    ifs2.close();
    ifs3.close();
    ifs4.close();
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
 * Create tables on start
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
        << "\"LOSES\" integer not null )";
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
        << "\"SPG\" integer not null,"
        << "\"BPG\" integer not null )";
    txn.exec(ssp.str());
    txn.commit();
}

/**
 * fill table specified by name with data from files also specified by name
 * @param conn connection with db
 * @param name name of src file
 */
void fillTables(pqxx::connection & conn, const string & name) {
    cout << "The name : " << name << endl;
    size_t dot_pos = name.find_first_of('.');
    string tb_name = name.substr(0, dot_pos);
    transform(tb_name.begin(), tb_name.end(), tb_name.begin(), ::toupper);
    cout << "Tb name : " << tb_name << endl;
    
    if (tb_name == "PLAYER") {
        parsePlayer(tb_name, conn);
    }
    else if (tb_name == "TEAM") {
        parseTeam(tb_name, conn);
    }
    else if (tb_name == "STATE") {
        parseState(tb_name, conn);
    }
    else if (tb_name == "COLOR") {
        parseColor(tb_name, conn);
    }
    else {
        cerr << "No such table, check your src files." << endl;
        exit(1);
    }
}

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
            add_player(&C, team_id, jersey_num, first_name, last_name, mpg, ppg, rpg, apg, spg, bpg);
        }
    }
}

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

void parseColor(const string & name, connection & C) {
    ifstream input(name);
    if (input.is_open()) {
        string line;
        while (getline(input, line)) {
            stringstream liness(line);
            int color_id;
            string name;
            liness >> color_id >> name;
            add_state(&C, name);
        }
    }
}
