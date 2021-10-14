// #include <stdio.h>
// #include <iostream>

// int main(){
//     FILE *fp = fopen("/home/wang/cse-lab/test.txt", "w");
//     char str[] = "string";
//     fseek(fp, 20, SEEK_SET);
//     int a = fprintf(fp, "%s", str);
//     std::cout << a << std::endl;
//     fclose(fp);
//     return 0;
// }
#include <syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>

int main(){
    char filename[] = "/home/wang/cse-lab/test.txt";
    int fd = open(filename, O_CREAT|O_RDWR, 0);
    lseek(fd, 20, SEEK_SET);
    size_t a = write(fd, filename, 10);
    std::cout << a << std::endl;
    close(fd);
    return 0;
}