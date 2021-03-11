#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/signal.h>
#include <signal.h>


#define BUFFSIZE 100

typedef struct arg_struct {
    int arg1;
    char* vectrfile;
    int g;
};

typedef struct a_st {
    int n;
    int k;
    int l;
    char* vectrfile;
};

typedef struct item{
    int row;
    int col;
    int val;
};
typedef struct buffer {
    int in;
    int out;
    int count;
    struct item *data;
    int splitS;
};
struct sem {
    struct buffer * sdp; 
    sem_t *sem_mutex;
    sem_t *sem_full;            
    sem_t *sem_empty;          
};

//global variables
int **thread_sol;
int *all_answers;
struct sem **buf; 


//mapper thread
void * mapper_threads (struct arg_struct *  a)
{
    int row_i = a->arg1;
    char* vectorFile = a->vectrfile;
    int n = a->g;
  
    sem_t *sem_mutex  = buf[row_i-1]->sem_mutex;    
    sem_t *sem_full= buf[row_i-1]->sem_full;     
    sem_t *sem_empty = buf[row_i-1]->sem_empty;      
    struct buffer * sdp = buf[row_i-1]->sdp;

    // Read the ith split file
    // Create the split file name
    char split_file_name[100];
    char split_file_num[100];

    sprintf(split_file_num , "%d" , row_i);

    // Concatonate strings to make file name
    strcpy(split_file_name , "split_file_");
    strcat(split_file_name , split_file_num);
    strcat(split_file_name , ".txt");

    // Open the split file
    FILE *s_file = fopen (split_file_name, "r");
    int size = 0;
    if (s_file != NULL) {
        fseek (s_file, 0, SEEK_END);
        size = ftell(s_file);
    }
    rewind(s_file);
    char new_single_line[150];
     while(size != 0 && !feof(s_file)) {

        fgets(new_single_line , 150 , s_file);
	
	sem_wait(sem_empty);
        sem_wait(sem_mutex);

        // Return first token  
        char *token = strtok(new_single_line, " ");
        int i_index = atoi(token);
        // Return second token
        token = strtok(NULL, " ");
        int j_index = atoi(token);
        // Return third token
        token = strtok(NULL, " ");
        int mij_value = atoi(token);
	
	//putting into buffer
	sdp->data[sdp->in].row = i_index;       
        sdp->data[sdp->in].col = j_index;      
        sdp->data[sdp->in].val = mij_value;    
        sdp->in = (sdp->in + 1) % BUFFSIZE;
        sdp->count++;                // Count the number of elements in buffer
	// Wait for the reducer to acquire an element from the buffer
        sem_post(sem_mutex);
        sem_post(sem_full);

        // Calculate the partial answer and store it
        //thread_sol[row_i-1][i_index - 1] += mij_value * v_array[j_index - 1];  
     
    }

  fclose(s_file);
  //pthread_exit(NULL);
}

void * reducer_thread (struct a_st *  a)
{
    
    int n = a->n;
    int k = a->k;
    int l = a->l;
    int row;
    int col;
    int val;
    char* vectorFile = a->vectrfile;
    int final = 0;

    int v_array[n];
    FILE *v_file = fopen (vectorFile, "r");
    char singleLine[150];
    while(!feof(v_file)) {
        fgets(singleLine , 150 , v_file);
        // Return first token
	  
        char *token = strtok(singleLine, " ");
        int index = atoi(token);

        // Return second token
        token = strtok(NULL, " ");
        int value = atoi(token);

        // Store the value at the index
        v_array[index - 1] = value;
        
    }
    fclose(v_file);


    while(1){
	for(int i=0;i<k;i++){
	    if (buf[i]->sdp->count == 0) 
                continue;
            sem_wait(buf[i]->sem_full);
            sem_wait(buf[i]->sem_mutex);

	    row = buf[i]->sdp->data[buf[i]->sdp->out].row;
            col = buf[i]->sdp->data[buf[i]->sdp->out].col;
            val = buf[i]->sdp->data[buf[i]->sdp->out].val;
	    thread_sol[i][row - 1] += val * v_array[col - 1];
	
 	    buf[i]->sdp->out = (buf[i]->sdp->out + 1) % BUFFSIZE;
            buf[i]->sdp->count--;

	    sem_post(buf[i]->sem_mutex);
    	    sem_post(buf[i]->sem_empty);
            final++;
	}
	
	if(final == l)
	    break;
    }
    

    for(int i = 0; i < k; i++){
	for (int j = 0; j < n; j++){
	    all_answers[j] += thread_sol[i][j];
	}
    }
}



int main (int argc, char** argv) {
    clock_t tim; 
    
    tim = clock();
    // If the number of arguments are not 5 exit 
    if (argc != 5) {
        printf("The arguments passed are not correct.");
        exit(1);
    }

    char* matrixFile = argv[1];
    char* vectorFile = argv[2];
    char* resultFile = argv[3];
    int k = atoi(argv[4]);


    // Read the whole matrix file, find L (lines in the file) and calculate the value of s;
    FILE *m_file = fopen (matrixFile, "r"); 
    char singleLine[150];
    int l = 0;
    int s = 0;
	
    if(m_file == NULL) {
	printf("matrix file not opened\n");
    }
    while(!feof(m_file)) {
        fgets(singleLine , 150 , m_file);
        l++;
    }

    // Ceiling Function
    s = l/k;
    if (l > s * k)
        s++;
    //int rem = l - (s*k);

    // Check if k is smaller than l otherwise s is 0
    if (s == 0) {
        printf("K is bigger than number of lines");
        exit(1);
    }

    // close file 
    fclose (m_file); 

    // Generate Split Files
    
    // Make an Array of Files
    FILE* split_files[k];
    
    // Open all the files
    for (int i = 0 ; i < k ; i++) {

        // Create the split file name
        char split_file_name[100];
        char split_file_num[100];

        // Convert integer i to string
        sprintf(split_file_num , "%d" , (i+1));

        // Concatonate strings to make file name
        strcpy(split_file_name , "split_file_");
        strcat(split_file_name , split_file_num);
        strcat(split_file_name , ".txt");

        // Make and store the files
        split_files[i] = fopen(split_file_name, "w+");
    }


    // Store the values in the split files
    m_file = fopen (matrixFile, "r"); 
    int count = 1;
    int index = 0;

    while(!feof(m_file)) {
        fgets(singleLine , 150 , m_file);

        // If S lines are read && count < s*k
        // remove the new line character on the last value
        if ((count % s == 0 ) || count == l) {
            
            char* token = strtok(singleLine , "\n");
            fputs(token , split_files[index]);
            
            // Close the file
            fclose(split_files[index]);
            index++;
        } else {
            // Store the value in the files
            fputs(singleLine , split_files[index]);
        }

        count++;
    }


    // Close the matrix file
    fclose(m_file);

    // Count the length of the vector
    int n = 0;
    FILE *v_file = fopen(vectorFile, "r");
    while(!feof(v_file)) {
        fgets(singleLine , 150 , v_file);
        n++;
    }
    fclose(v_file);
    thread_sol = (int **)malloc(k*sizeof(int *));
    for(int i=0;i<n;i++){
        thread_sol[i] = (int *)malloc(n * sizeof(int));
    }
    for (int i = 0 ; i < k ; i++) {
	for(int j = 0; j<n; j++){
	    thread_sol[i][j] = 0;
	}
    }
    all_answers = (int*)malloc(n*sizeof(int));
    for(int i = 0; i<n;i++){
    	all_answers[i] = 0;
    }
    
    buf = (struct sem **)malloc(k*sizeof(struct sem *));
    for(int i=0;i<k;i++){
        buf[i] = (struct sem *)malloc(sizeof(struct sem));
	buf[i]->sdp = (struct buffer*)malloc(k * sizeof(struct buffer));
  	buf[i]->sdp->in = 0;
	buf[i]->sdp->out = 0;
	buf[i]->sdp->count = 0;
        buf[i]->sdp->splitS = s;
        buf[i]->sdp->data = (struct item *)malloc(s * sizeof(struct item));
	sem_t sem_mutex;
        sem_t sem_empty;
        sem_t sem_full;
	int xx = sem_init(&sem_mutex, 0, 1);
	buf[i]->sem_mutex = &sem_mutex;
	xx = sem_init(&sem_full, 0, 0);
	buf[i]->sem_full = &sem_full;
        xx = sem_init(&sem_empty, 0, BUFFSIZE);
	buf[i]->sem_empty = &sem_empty;
    }
    
    //mapper thread
    pthread_t t_id[k];
    for (int i = 0;i < k; i++)
    {
        struct arg_struct *argus;
        argus = (struct arg_struct*)malloc(sizeof(struct arg_struct));
        argus->arg1 = i+1;
	argus->vectrfile = vectorFile;
	argus->g = n;
	//pthread_t thread_id;  
    	pthread_create(&t_id[i], NULL, mapper_threads, (void *)argus); 
	//t_id[i] = thread_id;
    }
    
    //reducer thread
    struct a_st *fin;
    fin = (struct a_st*)malloc(sizeof(struct a_st));
    fin->n=n;
    fin->k=k;
    fin->l = l;
    fin->vectrfile = vectorFile;
    pthread_t thread_id;
    pthread_create(&thread_id, NULL, reducer_thread, (void *)fin);

    //joining threads
    for (int i = 0 ; i < k ; i ++ ) {
	pthread_join(t_id[i], NULL);
    }

    //pthread_exit(NULL);   
    pthread_join(thread_id, NULL);
    //pthread_exit(NULL);

 

    // Store the value in the output file
    FILE *outputFile = fopen( resultFile , "w+");

    for (int i = 0 ; i < n ; i++) {
        // Store the value in the file
        char answer_index[100];
        char answer_value[100];

        sprintf(answer_index , "%d" , (i+1));
        strcpy(answer_value , "");
        strcat(answer_value , answer_index);
        strcat(answer_value , " " );

        sprintf(answer_index , "%d" , all_answers[i]);
        strcat(answer_value , answer_index);
        strcat(answer_value , "\n");

        fputs(answer_value , outputFile);
    }
    //free(thread_sol);
    //free(all_answers);
    // Close the Output file
    fclose(outputFile);
    tim = clock() - tim; 
    double time_taken = ((double)tim)/CLOCKS_PER_SEC;
    printf("mvt took seconds to execute:  %f \n", time_taken);

    return (0);
}


