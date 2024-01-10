#include<stdio.h>
#include<mpi.h>
#include<stdlib.h>
#include<math.h>
#include<string.h>
#include<sys/time.h>
#include<time.h>

int fullSize;

int** readFile(char* filename, int** arr){
	FILE *file = fopen(filename, "r");
	if (file == NULL){
		printf("Error in opening input file\n");
		return NULL;
	}
	
	int num;
	fullSize = 0;
	while (fscanf(file, "%d\n", &num) != EOF){
		fullSize++;
	}
	
	fullSize = sqrt(fullSize);
	
	arr = (int**)malloc(fullSize*sizeof(int*));
	for (int x = 0; x < fullSize; x++){
		arr[x] = (int*)malloc(fullSize*sizeof(int));
	}
	
	int i = 0, j = 0;
	FILE *file1 = fopen(filename, "r");
	while (fscanf(file1, "%d\n", &num) != EOF && i < fullSize){
		arr[i][j] = num;
		j++;
		if (j == fullSize){
			j = 0;
			i++;
		}
	}
	fclose(file);
	fclose(file1);
	
	return arr;
}

void writeFile(char* filename, int** arr){
	FILE *file = fopen(filename, "w");
	if (file == NULL){
		printf("Error in opening output file\n");
		return;
	}
	
	for (int i = 0; i < fullSize; i++){
		for (int j = 0; j < fullSize; j++){
			fprintf(file, "%d\n", arr[i][j]);
		}
	}
}

int* read_and_concatenate(char** fileNames, int numFiles, int* totalNumInts) {
    int* concatenatedArray = NULL;
    int totalInts = 0;
    int i, j;
    char buffer[256];

    for (i = 0; i < numFiles; i++) {
        FILE* file = fopen(fileNames[i], "r");
        if (!file) {
            perror("Error opening file");
            exit(EXIT_FAILURE);
        }

        // Count the number of integers in the file
        int numInts = 0;
        while (fgets(buffer, sizeof(buffer), file)) {
            char* token = strtok(buffer, " ");
            while (token != NULL) {
                numInts++;
                token = strtok(NULL, " ");
            }
        }

        // Allocate memory for the concatenated array
        concatenatedArray = realloc(concatenatedArray, (totalInts + numInts) * sizeof(int));
        if (!concatenatedArray) {
            perror("Error allocating memory");
            exit(EXIT_FAILURE);
        }

        // Read the integers from the file and append to the concatenated array
        rewind(file);
        j = 0;
        while (fgets(buffer, sizeof(buffer), file)) {
            char* token = strtok(buffer, " ");
            while (token != NULL) {
                concatenatedArray[totalInts + j] = atoi(token);
                j++;
                token = strtok(NULL, " ");
            }
        }
        totalInts += numInts;

        fclose(file);
    }

    *totalNumInts = totalInts;
    return concatenatedArray;
}

int main(int argc, char* argv[]){
	int myrank, nprocs;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	
	//If need to change for even/odd processors, change mappers formula and or mapperA
	//0	1 2 3 4 5 6 7 8     mappers:8 mapperA:4
	
	
	int division;
	int mappers = nprocs -1;
	int mapperA = mappers/2;
	
	if (myrank == 0){
		int **arrA;
		int **arrB;
		arrA = readFile("ArrayA.txt",arrA);
		arrB = readFile("ArrayB.txt",arrB);
		
		int row = 0;
		division = fullSize/mapperA;
		for (int i = 1; i <= mappers; i++){
			MPI_Send(&fullSize, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		
		for (int i = 1; i <= mappers; i++){
			int fixingAck = 1;
			MPI_Recv(&fixingAck,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		}
		for (int i = 1; i <= mappers; i++){
			MPI_Send(&division, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		
		
		for (int i = 1; i < mapperA+1; i++){
			int matrix = 0;
			MPI_Send(&matrix, 1, MPI_INT, i, i, MPI_COMM_WORLD);
			int fixingAck = 1;
			MPI_Recv(&fixingAck,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			int *divArr = (int*)malloc(division*fullSize*sizeof(int));
			int x = 0;
			for (x = 0; x < division; x++){
				for (int j = 0; j < fullSize; j++){
					divArr[x*fullSize + j] = arrA[x+row][j];
				}
			}
			row += x;
			MPI_Send(divArr, division*fullSize, MPI_INT, i, i, MPI_COMM_WORLD);
			fixingAck = 1;
			MPI_Recv(&fixingAck,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		}
		row = 0;
		for (int i = mapperA+1; i < nprocs; i++){
			int matrix = 1;
			MPI_Send(&matrix, 1, MPI_INT, i, i, MPI_COMM_WORLD);
			int fixingAck = 1;
			MPI_Recv(&fixingAck,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			int *divArr = (int*)malloc(fullSize*division*sizeof(int));
			int zz = 0;
			for (zz = 0;zz < fullSize; zz++){
				for (int j = 0; j < division; j++){
					divArr[division*zz + j] = arrB[zz][j+row*division];
				}
			}
			row++;
			MPI_Send(divArr, division*fullSize, MPI_INT, i, i, MPI_COMM_WORLD);
			fixingAck = 1;
			MPI_Recv(&fixingAck,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		}
		
		int numKeys = mappers;
		int *keys = (int*)malloc(numKeys*sizeof(int));
		for (int i = 1; i < nprocs; i++){
			MPI_Recv(&keys[i-1], 1, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			int ack = 1;
			MPI_Send(&ack, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		
		
		int** values = (int**)malloc(mappers*sizeof(int*));
		for (int i = 0; i < mappers; i++){
			values[i] = (int*)malloc(fullSize*division*sizeof(int));
		}
		
		
		for (int i = 1; i < nprocs; i++){
			MPI_Recv(values[i-1], division*fullSize, MPI_INT, i, i, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
		
		
		
		int isReducer = 1;
		for (int i = 1; i < mapperA+1; i++){
			
			MPI_Send(&isReducer, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		int notReducer = 0;
		for (int i = mapperA+1; i < nprocs; i++){
			
			MPI_Send(&notReducer, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		
		int* key1 = (int*)malloc((numKeys/2)*sizeof(int));
		int** value1 = (int**)malloc((mappers/2)*sizeof(int*));
		for (int i = 0; i < mappers/2; i++){
			value1[i] = (int*)malloc(fullSize*division*sizeof(int));
		}
		
		
		//0 1 2 3 0 1 2 3
		for (int i = 0; i < numKeys/2; i++){
			key1[i] = keys[i];
			value1[i] = values[i];
			
		}
		
		
		
		int* key2 = (int*)malloc((numKeys/2)*sizeof(int));
		int** value2 = (int**)malloc((mapperA)*sizeof(int*));
		for (int i = 0; i < mapperA; i++){
			value2[i] = (int*)malloc(fullSize*division*sizeof(int));
		}
		for (int i = numKeys/2; i < numKeys; i++){
			key2[i - numKeys/2] = keys[i];
			value2[i - numKeys/2] = values[i];
		}
		
		for (int i = 1; i < mapperA+1; i++){
			MPI_Send(&key1[i-1], 1, MPI_INT, i, i, MPI_COMM_WORLD);
			int ack = 0;
			MPI_Recv(&ack,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Send(&key2[i-1], 1, MPI_INT, i, i, MPI_COMM_WORLD);
			
			MPI_Send(value1[i-1], fullSize*division, MPI_INT, i, i, MPI_COMM_WORLD);
			MPI_Recv(&ack,1,MPI_INT,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Send(value2[i-1], fullSize*division, MPI_INT, i, i, MPI_COMM_WORLD);
			
		}
		
		
		char** fileNames = (char**)malloc(mapperA*sizeof(char*));
		for (int i = 0; i < mapperA; i++){
			fileNames[i] = (char*)malloc(20*sizeof(char));
		}
		
		for (int i = 1; i < mapperA+1; i++){
			//printf("Reading from %d\n",i);
			MPI_Recv(fileNames[i-1],20,MPI_CHAR,i,i,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			//printf("Done Reading from %d\n",i);
		}
		
		int* matrix = (int*)malloc(fullSize+4);
		int totalInts = 0;
		matrix = read_and_concatenate(fileNames, mapperA, &totalInts);
		printf("Final Matrix: ");
		for (int i = 0; i < fullSize; i++){
			printf("%d ", matrix[i]);
		}
		printf("\n");
	}
	else{
		int fullSize1 = 0;
		int division1 = 0;
		MPI_Recv(&fullSize1,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		int ackFix = 1;
		MPI_Send(&ackFix, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
		MPI_Recv(&division1,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		int matrix = 0;
		MPI_Recv(&matrix,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		ackFix = 1;
		MPI_Send(&ackFix, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
		if (matrix == 0){
			int *divA = (int*)malloc(division1*fullSize1*sizeof(int));
			
			int divASize = division1*fullSize1;
			MPI_Recv(divA, divASize, MPI_INT, 0, myrank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			int *newdivA = (int*)malloc(division1*fullSize1*sizeof(int));
			for (int i = 0; i < fullSize1; i++){
    				for (int j = 0; j < division1; j++){
        				newdivA[i*division1 + j] = divA[j*fullSize1 + i];
    				}
			}
			
			ackFix = 1;
			MPI_Send(&ackFix, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
			
			int key = myrank - 1;
			int ack = 0;
			MPI_Send(&key, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
			MPI_Recv(&ack,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Send(newdivA,fullSize1*division1,MPI_INT,0,myrank,MPI_COMM_WORLD);
		}
		else{
			int *divB = (int*)malloc(division1*fullSize1*sizeof(int));
			
			int divBSize = division1*fullSize1;
			
			MPI_Recv(divB, divBSize, MPI_INT, 0, myrank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			ackFix = 1;
			MPI_Send(&ackFix, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
			
			//printf("%d Reached end\n", myrank);
			int key = (myrank - 1) % mapperA;
			int ack = 0;
			MPI_Send(&key, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
			MPI_Recv(&ack,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			MPI_Send(divB,fullSize1*division1,MPI_INT,0,myrank,MPI_COMM_WORLD);
		}
		
		int reducer = 0;
		MPI_Recv(&reducer,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		
		if (reducer == 1){
			int key1 = 0,key2 = 0;
			MPI_Recv(&key1,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			int ack2 = 1;
			MPI_Send(&ack2, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
			MPI_Recv(&key2,1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
			
			if (key1 != key2){
				printf("%d and %d Keys not equal\n", key1, key2);
				
			}
			else{
				int *valA = (int*)malloc(fullSize1*division1*sizeof(int));
			
				int *valB = (int*)malloc(fullSize1*division1*sizeof(int));
				
				MPI_Recv(valA,fullSize1*division1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				ack2 = 1;
				MPI_Send(&ack2, 1, MPI_INT, 0, myrank, MPI_COMM_WORLD);
				MPI_Recv(valB,fullSize1*division1,MPI_INT,0,myrank,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
				
				int* dotProd = (int*)malloc(fullSize1*sizeof(int));
				
				for (int i = 0; i < fullSize1; i++){
					dotProd[i] = 0;
					for (int j = 0; j < division1; j++){
						dotProd[i] += valA[i*division1 + j]*valB[i*division1 + j];
					}
				}
				
				int *keyValItem = (int*)malloc((fullSize1+1)*sizeof(int));
				keyValItem[0] = key1;
				for (int i = 1; i < fullSize1+1; i++){
					keyValItem[i] = dotProd[i-1];
				}
				
				char* filename = (char*)malloc(20*sizeof(char));
				
				sprintf(filename,"keyVal%d.txt",myrank);
				FILE* reducerfile = fopen(filename,"w, ccs=UTF-8");
				
				for (int k = 0; k < fullSize1+1; k++){
					fprintf(reducerfile, "%d\n", keyValItem[k]);
				}
				fclose(reducerfile);
				
				MPI_Send(filename, 20*sizeof(char), MPI_CHAR,0,myrank,MPI_COMM_WORLD);
				
				//Write key value pairs to file and master to read them and concat column wise.
			}
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
}
