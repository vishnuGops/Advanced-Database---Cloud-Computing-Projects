I = LOAD '$G' using PigStorage(',') AS (a:int , b:int);
G = group I by a;
Inc = foreach G generate group, COUNT(I);
S = foreach Inc generate $1 as X, $0 as Y;
R = group S by X;
O = foreach R generate group, COUNT(S);
store O into '$O' using PigStorage(',');