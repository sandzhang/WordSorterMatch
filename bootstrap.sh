#g++ -o main -O3 -g main.cpp -lpthread && time ./main 16 sowpods.txt out.txt
#g++ -o main_sp -O3 -g main_sp.cpp -lpthread && time ./main_sp 16 sowpods.txt out.txt
#g++ -o main_sp -O3 -g main_sp.cpp -lpthread && time ./main_sp 16 mini.txt out.txt
g++ -o main_sp2 -O3 -g main_sp2.cpp -lpthread && time ./main_sp2 16 sowpods.txt out.txt
#g++ -o main_sp2 -O3 -g main_sp2.cpp -lpthread && time ./main_sp2 16 mini.txt out.txt
#g++ -o main_sp2 -O3 -g main_sp2.cpp -lpthread && time ./main_sp2 16 mini3.txt out.txt
