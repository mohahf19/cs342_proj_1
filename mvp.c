#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <wait.h>
#include <math.h>
#include <string.h>
#include <fcntl.h>

#define JOIN(a, b) (a ## b)
// WARNING: don't LOOP in the same line
#define loop(n) for (unsigned JOIN(HIDDEN, __LINE__) = 0; JOIN(HIDDEN, __LINE__) < n; JOIN(HIDDEN, __LINE__)++)

int countLines(char *matrixfile);

void createSplits(char *matrixfile, int s, int k , int l, int *);

void createAndProcessSplits(int k, char *vectorfile);

void printarr(int *pInt, int k);

void mapperProcess(int i, char *vectorfile);

int *readVector(char *vectorfile, int *numLines);

int * initEmptyArr(int n);

void printResult(int *arr, int n, int i, char *);

void combineAndWriteResults(int created, char *resultfile, char* vec);

void deleteMiddleFiles(int created);

void writeToPipe(int* res, int n,int i, char* inter);

int main(int argc, char *argv[]) {

    if (argc == 5){
        char* matrixfile = argv[1];
        char* vectorfile = argv[2];
        char* resultfile = argv[3];
        char* partitions = argv[4];
        int k, s;
        int filesCreated;
        printf("%s\n%s\n%s\n%s\n", matrixfile, vectorfile, resultfile, partitions);


        int lineCount = countLines(matrixfile);
        printf("%s has %d lines!\n", matrixfile, lineCount);
        sscanf(partitions, "%d", &k);
        printf("i need %d partitions, so %d\n = ceil(%f)", k,
               (int) ceil(1.0 * lineCount/k), 1.0 * lineCount / k);
        s = (int) (ceil(1.0 * lineCount / k));

        createSplits(matrixfile, s, k, lineCount, &filesCreated);

        createAndProcessSplits(filesCreated, vectorfile);

        combineAndWriteResults(filesCreated, resultfile, vectorfile);

        deleteMiddleFiles(filesCreated);

    } else{
        printf("missing parameters!\n");
    }
    return 0;
}

void deleteMiddleFiles(int created) {
    for(int i = 0; i < created; i++){
        char buf[7];
        snprintf(buf, 7, "inter%d", i);
        remove(buf);

        snprintf(buf, 7, "split%d", i);
        remove(buf);
    }

    printf("Done deleting! bybye.\n");

}


void combineAndWriteResults(int created, char* resultName, char* vector) {
    pid_t n = 0;

    n = fork();
    if (n < 0){
        printf("Fork failed:( \n");
        exit(-1);
    } else if (n == 0){ //reducer process
        int n = countLines(vector);
        int * result = initEmptyArr(n);

        for (int i = 0; i < created; i++){
            char buf[7];
            snprintf(buf, 7, "inter%d", i);
            FILE *inter = fopen(buf, "r");

            char* line;
            size_t len = 0;
            ssize_t read;

            while( (read = getline(&line, &len, inter) != -1)){
                int row, val;
                sscanf(line, "%d%d\n", &row, &val);
                result[row-1] += val;
            }
            fclose(inter);
        }
        printarr(result, n);
        printResult(result, n, -1, resultName);
        exit(0);
    } //child end

    wait(NULL);
    printf("Writing result done! Thanks for using meeee. \n");
}

void createAndProcessSplits(int files, char *vectorfile) {
    pid_t n = 0;

    for(int i = 0; i < files; i++){
        n = fork();
        if (n < 0){
            printf("Fork failed. :(\n");
            exit(-1);
        } else if (n == 0){ //child
            mapperProcess(i, vectorfile);
        }
    }

    loop(files){
        wait(NULL);
    }
}

void mapperProcess(int i, char *vectorfile) {
    //create a pipe and then write to it the processed data
    int n;
    int * vec;
    int * res;

    printf("hi\n");
    vec = readVector(vectorfile, &n);
    res = initEmptyArr( n);
    printarr(res, n);
    printarr(vec, n);
    printf("child %d found %d values in vector.\n", i, n);

    char buf[7];
    snprintf(buf, 7, "split%d", i);
    FILE *split = fopen(buf, "r");

    char*line = NULL;
    size_t len = 0;
    ssize_t read ;

    printf("beginning to read..\n");
    int row, col, val;
    while ((read = getline(&line, &len, split) != -1)) {
        printf("Read: %s\n", line);
        for(int i = 0; i < 6; i++){
            printf("|%c|\n", line[i]);
        }

        
        sscanf(line, "%d%d%d\n", &row, &col, &val);
        res[row-1] = res[row-1] + (val * vec[col-1]);
    }

    fclose(split);
    printf("in child %d, result is: ", i);
    printarr(res, n);

    //printing to files
    char inter []= "./inter";
    writeToPipe(res, n, i, inter);
    free(vec);
    free(res);
    
    exit(0);
}

void writeToPipe(int* res, int n,int i, char* inter){
    //open the ith pipe

    char buf[9];
    snprintf(buf, 9, "./inter%d", i);
    int piperes = mkfifo(buf, O_WONLY);

    //write the array to it
}

void printResult(int *arr, int n, int i, char* filename) {
    char buf[7];
    if ( i >=0 ){ 
        snprintf(buf, 7, "%s%d",filename,  i);
    } else{ //i < 0 is true for the end result file
        snprintf(buf, 7, "%s",filename);
    }

    printf("file name is: %s\n", buf);
    FILE *fp = fopen(buf, "w");
    for(int i = 0; i < n; i++){
        if (i < 0) {
            snprintf(buf, 7, "%d %d\n", i + 1, arr[i]);
            fputs(buf, fp);
        } else{
            if (arr[i] != 0){
                snprintf(buf, 7, "%d %d\n", i + 1, arr[i]);
                fputs(buf, fp);
            }
        }
    }

    fclose(fp);
}

int* initEmptyArr(int n) {
    int* arr;
    printf("initializing array..\n");
    arr = (int *) malloc(sizeof(int) * n);
    for(int i = 0; i < n; i++){
        arr[i] = 0;
    }
    return arr;
}

int *readVector(char *vectorfile, int *numLines) {
    printf("counting lines..\n");
    int n = countLines(vectorfile);
    *numLines = n;
    printf("found %d many lines.\n", n);
    //make int arr and start filling it
    int * arr = (int *) malloc(sizeof(int) * (n));
    
    FILE *fp = fopen(vectorfile, "r");

    char*line = NULL;
    size_t len = 0;
    ssize_t read;
    int linesread = 0;

    while ((read = getline(&line, &len, fp) != -1)) {
        int row, val;
        printf("Read: %s\n", line);
        sscanf(line,"%d%d\n", &row, &val);
        arr[row-1] = val;
        linesread++;
    }
    return arr;
}

void printarr(int *arr, int k) {
    printf("[");
    for(int i = 0; i < k -1; i++){
        printf("%d, ", arr[i]);
    }
    printf("%d]\n", arr[k-1]);
}

void createSplits(char *matrixfile, int s, int k, int l , int* filesCreated) {
    FILE *fp = fopen(matrixfile, "r");
    if (fp == NULL){
        printf("Couldn't open %s\n", matrixfile);
    }
    char*line = NULL;
    size_t len = 0;
    ssize_t read;
    int linesread = 0;


    for(int i = 0; i < k; i++){
        if (linesread < l) {
            int sofar = 0;
            char buf[7];
            snprintf(buf, 7, "split%d", i);
            printf("file name is: %s\n", buf);
            FILE *out = fopen(buf, "w");
            if (out == NULL){
                printf("couldn't open file %s\n", buf);
                exit(-1);
            }
            while (sofar < s && (read = getline(&line, &len, fp) != -1)) {
                printf("Read: %s\n", line);
                fputs(line, out);
                sofar++;
                linesread++;
            }
            printf("done writing %s\n", buf);
            fclose(out);
            *filesCreated = i + 1;
        }
    }

    free(line);
    fclose(fp);
}

int countLines(char *matrixfile) {
    FILE *fp = fopen(matrixfile, "r");
    if (fp == NULL){
        printf("Couldn't find file %s", matrixfile);
    }
    int count = 0;

    if (fp == NULL){
        printf("File %s does not exist.\n", matrixfile);
        exit(-1);
    } else {
        printf("counting file %s\n", matrixfile);

        char*line = NULL;
        size_t len = 0;

        while ((getline(&line, &len, fp) != -1)) {
            count++;
        }

//
//        while((ch=fgetc(fp)) !=EOF){
//            if (ch == '\n')
//                count++;
//        }
    }

    fclose(fp);
    return count;
}