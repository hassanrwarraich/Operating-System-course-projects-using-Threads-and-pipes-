#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <sys/types.h>
#include <sys/wait.h>

void mapper_function(int row_i , char* vectorFile, int n);

int main (int argc, char** argv) {

    printf ("mv started\n");

    // Proccess the arguments

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

    while(!feof(m_file)) {
        fgets(singleLine , 150 , m_file);
        l++;
    }

    // Ceiling Function
    s = l/k;
    if (l > s * k)
        s++;

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

        // If S lines are read
        // remove the new line character on the last value
        if (count % s == 0 || count == l) {
            
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

    // Creating Mappers By Forking Children
    for(int i = 0;i < k; i++)  
    { 
        if(fork() == 0) 
        {   
            mapper_function(i+1 , vectorFile , n);
            exit(0); 
        } 
    } 
    for(int i=0;i<5;i++) 
        wait(NULL); 


    // Merge all the mappers with a reducer
    // Read all the files from partial answer and produce one single output file
    // Make an Array of Files
    char answers_singleLine[150];
    int all_answers[n];
    for (int i = 0 ; i < n ; i++) {
        all_answers[i] = 0;
    }

    // Open all the files
    for (int i = 0 ; i < k ; i++) {

        // Create the split file name
        char partial_ans_name[100];
        char partial_ans_num[100];

        // Convert integer i to string
        sprintf(partial_ans_num , "%d" , (i+1));

        // Concatonate strings to make file name
        strcpy(partial_ans_name , "partial_answer_file_");
        strcat(partial_ans_name , partial_ans_num);
        strcat(partial_ans_name , ".txt");

        // Open and read the files
        FILE* p_file = fopen(partial_ans_name, "r");

        while(!feof(p_file)) {
            fgets(answers_singleLine , 150 , p_file);

            // printf("%s\n" , answers_singleLine);

            char *token = strtok(answers_singleLine, " ");
            int index = atoi(token);  
            // printf("%d " , index);

            token = strtok(NULL , " ");
            int value = atoi(token);
            // printf("%d " , value);

            all_answers[index - 1] += value;
            // printf("%d\n" , all_answers[index - 1]);
        }

        // Close the File
        fclose(p_file);
    }

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

    // Close the Output file
    fclose(outputFile);
    
    return (0);
}

void mapper_function(int row_i , char* vectorFile, int n) {

    // Open and store the vector file in an array
    FILE *v_file = fopen (vectorFile, "r"); 
    char singleLine[150];

    // Store the vector array
    int v_array[n];

    // Split the string to get the values of the vector
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
        
        // printf("%d\n" , v_array[index - 1]);
    }

    //  Close the vector file
    fclose(v_file);

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

    // Create the partial answer array and initialize it to 0
    int partial_answer_array[n];
    for (int i = 0 ; i < n ; i++) {
        partial_answer_array[i] = 0;
    }
    
    // Create a partial answer file  
    // Create the partial answer file name
    char partial_answer_name[100];
    char partial_answer_num[100];

    // Convert integer i to string
    sprintf(partial_answer_num , "%d" , row_i);

    // Concatonate strings to make file name
    strcpy(partial_answer_name , "partial_answer_file_");
    strcat(partial_answer_name , partial_answer_num);
    strcat(partial_answer_name , ".txt");

    FILE *p_file = fopen(partial_answer_name, "w+");
    char new_single_line[150];

    // Check if the file is empty
    int size = 0;
    if (s_file != NULL) {
        fseek (s_file, 0, SEEK_END);
        size = ftell(s_file);
    }

    // Return the pointer of the file back to the start of the file
    rewind(s_file);

    // Split the string to get the matrix values and calculate the partial answer
    while(size != 0 && !feof(s_file)) {
        
        fgets(new_single_line , 150 , s_file);
        
        // printf(new_single_line);

        // Return first token  
        char *token = strtok(new_single_line, " ");
        // printf("%s\n" , token);
        int i_index = atoi(token);

        // Return second token
        token = strtok(NULL, " ");
        // printf("%s\n", token);
        int j_index = atoi(token);

        // Return third token
        token = strtok(NULL, " ");
        // printf("%s\n", token);
        int mij_value = atoi(token);

        // Calculate the partial answer and store it
        partial_answer_array[i_index - 1] += mij_value * v_array[j_index - 1];
        // printf("%d %d %d %d " , i_index , j_index , mij_value , v_array[j_index - 1] );
        // printf("%d\n" , partial_answer_array[i_index - 1]);  
     
    }

    for (int i = 0 ; i < n ; i++) {
        // Store the value in the files
        char partial_answer_index[100];
        char partial_answer_value[100];

        sprintf(partial_answer_index , "%d" , (i+1));
        strcpy(partial_answer_value , "");
        strcat(partial_answer_value , partial_answer_index);
        strcat(partial_answer_value , " " );

        sprintf(partial_answer_index , "%d" , partial_answer_array[i]);
        strcat(partial_answer_value , partial_answer_index);
        strcat(partial_answer_value , "\n");

        // printf(partial_answer_value);
        if (i == n - 1) {
            char* token = strtok(partial_answer_value , "\n");
            fputs(token , p_file);
            
        } else
            fputs(partial_answer_value , p_file);
    }


    // Close the files
    fclose(s_file);
    fclose(p_file);
}