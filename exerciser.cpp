#include "exerciser.h"

void exercise(connection * C) {
    query1(C, 1, 35, 40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    query2(C, "Gold");
    query3(C, "Duke");
    query4(C, "IN", "Gold");
    query5(C, 11);
}
