#include "query_funcs.h"
#include <iomanip>
/**
 * find all single quotes in str and escape it by doubling it
 * @param str string to inspect
 */
void escapeSingle(string & str) {
    string temp = str;
    size_t pos = 0;
    size_t temp_pos = 0;
    while ((pos = temp.find('\'')) != string::npos) {
        temp_pos += pos;
        str.insert(temp_pos, "'");
        temp = temp.substr(pos + 1);
        temp_pos += 2;
    }
}


void add_player(connection *C, int team_id, int jersey_num, string first_name, string last_name,
                int mpg, int ppg, int rpg, int apg, double spg, double bpg) {
    escapeSingle(first_name);
    escapeSingle(last_name);
    nontransaction ntxn(*C);
    stringstream sql;
    sql << "insert into \"PLAYER\" (\"TEAM_ID\", \"UNIFORM_NUM\", \"FIRST_NAME\", \"LAST_NAME\", \"MPG\", \"PPG\", \"RPG\", \"APG\", \"SPG\", \"BPG\") values"
        << "(" << team_id << "," << jersey_num << "," << "'" << first_name << "','" << last_name << "'," << mpg << "," << ppg << "," << rpg << "," << apg << "," << spg << "," << bpg << ");" ;
    ntxn.exec(sql.str());
}


void add_team(connection *C, string name, int state_id, int color_id, int wins, int losses) {
    escapeSingle(name);
    nontransaction ntxn(*C);
    stringstream sql;
    sql << "insert into \"TEAM\" (\"NAME\", \"STATE_ID\", \"COLOR_ID\", \"WINS\", \"LOSSES\") values" 
        << "('" << name << "'," << state_id << "," << color_id << "," << wins << "," << losses << ");";
    ntxn.exec(sql.str());
}


void add_state(connection *C, string name) {
    escapeSingle(name);
    nontransaction ntxn(*C);
    stringstream sql;
    sql << "insert into \"STATE\" (\"NAME\") values" << "('" << name << "');" ;
    ntxn.exec(sql.str());
}


void add_color(connection *C, string name) {
    escapeSingle(name);
    nontransaction ntxn(*C);
    stringstream sql;
    sql << "insert into \"COLOR\" (\"NAME\") values" << "('" << name << "');" ;
    ntxn.exec(sql.str());
}


/**
 * Helper function to find the results of certain attr in range between min and max
 * @param txn transaction object
 * @param use flag to indicate use or not
 * @param min min of specified statistic
 * @param max max of specified statistic
 * @param attr specified statistic
 */
void query1Helper(int use, int min, int max, string attr, stringstream & sql, bool * done) {
    if (use) {
        if (*done) {
            sql << " and ";
        }
        else {
            sql << " where ";
        }
        sql << "(\"" << attr << "\" >= " << min << " and \"" << attr << "\" <= " << max << ")";
        *done = true;
    }
}

void query1Helper2(int use, double min, double max, string attr, stringstream & sql, bool * done) {
    if (use) {
        if (*done) {
            sql << " and ";
        }
        else {
            sql << " where ";
        }
        sql << "(\"" << attr << "\" >= " << min << " and \"" << attr << "\" <= " << max << ")";
        *done = true;
    }
}

void query1(connection *C,
	    int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            ) {
    cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG" << endl;
    stringstream sql;
    sql << "select * from \"PLAYER\" ";
    nontransaction txn(*C);
    bool done = false;
    query1Helper(use_mpg, min_mpg, max_mpg, "MPG", sql, &done);
    query1Helper(use_ppg, min_ppg, max_ppg, "PPG", sql, &done);
    query1Helper(use_rpg, min_rpg, max_rpg, "RPG", sql, &done);
    query1Helper(use_apg, min_apg, max_apg, "APG", sql, &done);
    query1Helper2(use_spg, min_spg, max_spg, "SPG", sql, &done);
    query1Helper2(use_bpg, min_bpg, max_bpg, "BPG", sql, &done);
    sql << ";";
    // cout << sql.str() << endl;
    result R = txn.exec(sql.str());
    /* List down all the records */
    for (auto const & r : R) {
        cout << r[0].as<int>() << " ";
        cout << r[1].as<int>() << " ";
        cout << r[2].as<int>() << " ";
        cout << r[3].as<string>() << " ";
        cout << r[4].as<string>() << " ";
        cout << r[5].as<int>() << " ";
        cout << r[6].as<int>() << " ";
        cout << r[7].as<int>() << " ";
        cout << r[8].as<int>() << " ";
        cout << fixed << setprecision(1);
        cout << r[9].as<double>() << " ";
        cout << r[10].as<double>() << endl;
    }
}


void query2(connection *C, string team_color) {
    cout << "NAME" << endl;
    nontransaction txn(*C);
    stringstream sql;
    sql << "select \"NAME\" from \"TEAM\" where \"COLOR_ID\" in (select \"COLOR_ID\" from  \"COLOR\" where \"NAME\"='" << team_color << "')";
    result R = txn.exec(sql.str());
    /* List down all the records */
    for (const pqxx::tuple & r : R) {
        cout << r[0].as<string>() << endl;
    }
}


void query3(connection *C, string team_name) {
    cout << "FIRST_NAME LAST_NAME" << endl;
    nontransaction txn(*C);
    stringstream sql;
    sql << "select \"FIRST_NAME\",\"LAST_NAME\" from \"PLAYER\",\"TEAM\" where " 
        << "\"PLAYER\".\"TEAM_ID\"=\"TEAM\".\"TEAM_ID\" and " 
        << "\"TEAM\".\"NAME\"='" << team_name << "' order by \"PPG\" desc;";
    // cout << sql.str() << endl;
    result R = txn.exec(sql.str());
    /* List down all the records */
    for (const pqxx::tuple & r : R) {
        cout << r[0].as<string>() << " ";
        cout << r[1].as<string>() << endl;
    }
}


void query4(connection *C, string team_state, string team_color) {
    cout << "FIRST_NAME LAST_NAME UNIFORM_NUM" << endl;
    nontransaction txn(*C);
    stringstream sql;
    sql << "select \"FIRST_NAME\",\"LAST_NAME\",\"UNIFORM_NUM\""
	    << "from \"PLAYER\" where \"TEAM_ID\" in ("
		<< "select \"TEAM_ID\" from \"TEAM\",\"STATE\",\"COLOR\" where" 
		<< "\"TEAM\".\"STATE_ID\"=\"STATE\".\"STATE_ID\" and \"STATE\".\"NAME\"='" << team_state << "' and"
		<< "\"TEAM\".\"COLOR_ID\"=\"COLOR\".\"COLOR_ID\" and \"COLOR\".\"NAME\"='" << team_color << "'"
	    << ");";
    result R = txn.exec(sql.str());
    /* List down all the records */
    for (const pqxx::tuple & r : R) {
        cout << r[0].as<string>() << " ";
        cout << r[1].as<string>() << " ";
        cout << r[2].as<int>() << endl;
    }
}


void query5(connection *C, int num_wins) {
    cout << "FIRST_NAME LAST_NAME NAME WINS" << endl;
    nontransaction txn(*C);
    stringstream sql;
    sql << "select \"FIRST_NAME\",\"LAST_NAME\",\"TEAM\".\"NAME\",\"WINS\" from \"PLAYER\",\"TEAM\"" 
        << "where \"PLAYER\".\"TEAM_ID\"=\"TEAM\".\"TEAM_ID\" and \"WINS\" > " << num_wins << ";";
    result R = txn.exec(sql.str());
    /* List down all the records */
    for (const pqxx::tuple & r : R) {
        cout << r[0].as<string>() << " ";
        cout << r[1].as<string>() << " ";
        cout << r[2].as<string>() << " ";
        cout << r[3].as<int>() << endl;
    }
}
