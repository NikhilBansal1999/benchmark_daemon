#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define IPC_PORT 50000
#define RPC_PORT 50001
#define IPLEN 16
#define MAX_HOST_NAME 128
#define SLEEP_TIME 900

typedef struct data
{
  int core_count;
  float cpu_speed;
  float latency;         /* bandwidth in Mb/s */
  float bandwidth;       /* latency in us */
  char ip_address[16];
  struct data* next;
} node_data;

struct send_data
{
  int core_count;
  float cpu_speed;
};

struct rpc
{
  char* my_ip;
  char* bench_dir;
};

node_data* head = NULL;
node_data* tail = NULL;
FILE* logfile;
pthread_mutex_t lock;

/*Checks if a node is pesent in the list of nodes*/
node_data* is_present(char* ip)
{
  if(head == NULL && tail == NULL)
  {
    return NULL;
  }
  else
  {
    node_data* temp_node;
    temp_node = head;
    while(temp_node != NULL)
    {
      if(strcmp(temp_node->ip_address, ip) == 0)
      {
        return temp_node;
      }
      else
      {
        temp_node = temp_node->next;
      }
    }
    return NULL;
  }
}

/*Add a node to the list of nodes*/
void Add(node_data* new_node)
{
  if(head == NULL && tail == NULL)
  {
    head = new_node;
    tail = new_node;
  }
  else
  {
    tail->next = new_node;
    tail = new_node;
  }
}

/*Get the number of cpu cores in the host*/
int get_core_count()
{
  int num_cpus = sysconf( _SC_NPROCESSORS_ONLN );
  return num_cpus;
}

/*Get the current data about about CPU Performance*/
float get_cpu_speed(char* my_ip, char* bench_dir)
{
  char outfile[24];
  char command[256];
  snprintf(outfile,24,"cpu_%s",my_ip); //Ensure the different output files are opened for different nodes
  //Form the command string and execute the CPU benchmark
  snprintf(command,256,"((timeout 0.5s %s/IMB-IO) | grep performance) > %s",bench_dir,outfile);
  if(system(command) != 0)
  {
    fprintf(logfile,"Error benchmarking CPU Speed\n");
    return 0;    //Benchmark can't be executed
  }
  //Read the output file to extract CPU benchmark results
  FILE* handle = fopen(outfile,"r");
  if(handle == NULL)
  {
    fprintf(logfile,"Error opening CPU Data file while reading results\n");
    return 0;
  }
  float cpu_speed;
  fscanf(handle,"# performance of  %f",&cpu_speed);
  if(fclose(handle) != 0)
  {
    fprintf(logfile,"Error closing CPU Data file\n");
  }
  return cpu_speed;
}

/*Get network latency between the current node and an another node*/
float get_latency(char* my_ip, char* host_ip, char* bench_dir, char* user)
{
  char outfile[24];
  char command[256];
  snprintf(outfile,24,"lat_%s",my_ip); //Ensure the different output files are opened for different nodes

  //Prepare a host file to supply to mpirun
  FILE* fhand = fopen(outfile,"w");
  if(fhand != NULL)
  {
    fprintf(logfile,"Error opening hostfile while measuring latency\n");
    return -1.0;   //Reuse previously measured latency in case of benchmark failure
  }
  fprintf(fhand,"%s user=%s\n",my_ip,user);
  fprintf(fhand,"%s user=%s\n",host_ip,user);
  if(fclose(fhand) != 0)
  {
    fprintf(logfile,"Error closing hostfile while measuring latency\n");
  }

  //Form the command string and execute the CPU benchmark
  snprintf(command,256,"(mpirun -n 2 -f ./%s %s/osu_latency -m 8:128 | sed '1,2d') > %s",outfile,bench_dir,outfile);
  if(system(command) != 0)
  {
    //Remote host is probably unreachable. Return a very high latency value to mark this
    fprintf(logfile,"Unable to benchmark latency for host %s\n",host_ip);
    return 100000.0;
  }

  //Read the output file to determine network latency
  FILE* handle = fopen(outfile,"r");
  if(handle == NULL)
  {
    fprintf(logfile,"Unable to open latency data file\n");
    return -1.0; //Reuse previous latency value
  }
  int field1;
  float field2, latency;
  latency = 0.0;
  for(int i=0;i<5;i++)
  {
    fscanf(handle,"%d %f\n",&field1,&field2);
    latency = latency + field2;
  }
  if(fclose(handle) != 0)
  {
    fprintf(logfile,"Unable to close latency data file\n");
  }
  latency = latency/5;
  return latency;
}

/*Get the network bandwidth*/
float get_bandwidth(char* my_ip, char* host_ip, char* bench_dir, char* user)
{
  char outfile[24];
  char outfile2[24];
  char command[256];
  snprintf(outfile,24,"band_%s",my_ip); //Ensure the different output files are opened for different nodes

  //Form a host file to supply to mpirun
  FILE* fhand = fopen(outfile,"w");
  if(fhand == NULL)
  {
    fprintf(logfile,"Unable to prepare hostfile for bandwidth benchmark\n");
    return -1.0; //Reuse previous data
  }
  fprintf(fhand,"%s user=%s\n",my_ip,user);
  fprintf(fhand,"%s user=%s\n",host_ip,user);
  if(fclose(fhand) != 0)
  {
    fprintf(logfile,"Unable to close hostfile for bandwidth benchmark\n");
  }

  //Form the command string for benchmark and execute it
  snprintf(outfile2,24,"ban_%s",my_ip); //Ensure the different output files are opened for different nodes
  snprintf(command,256,"(mpirun -n 2 -f ./%s %s/IMB-MPI1 PingPong | sed '1,55d' | sed '14,16d') > %s",outfile,bench_dir,outfile2);
  if(system(command) != 0)
  {
    /*unable to benchmark bandwidth, remote host is probably unreachable
      Return 0 bandwidth to mark the failure*/
    fprintf(logfile,"Unable to benchmark bandwidth for host %s \n",host_ip);
    return 0;
  }

  //open the output file to determine the bandwidth measurement results
  FILE* handle = fopen(outfile2,"r");
  if(handle == NULL)
  {
    fprintf(logfile,"Unable to open bandwidth data file\n");
    return -1.0; //Reuse previous data
  }
  int field1, field2;
  float field3, field4, bandwidth;
  bandwidth = 0.0;
  for(int i=0;i<13;i++)
  {
    fscanf(handle,"%d %d %f %f\n",&field1,&field2,&field3,&field4);
    bandwidth = bandwidth + field4;
  }
  bandwidth = bandwidth/13;
  if(fclose(handle) != 0)
  {
    fprintf(logfile,"Unable to close data file for bandwidth benchmark\n");
  }
  return bandwidth;
}

void* serve_ipc(void* params)
{
  int socketfd, newsocket_fd;
  struct sockaddr_in host_addr, client_addr;
  socklen_t sin_size;
  int recv_len=1, yes=1;
  char ip_ad[16];

  if((socketfd = socket(PF_INET,SOCK_STREAM,0)) == -1)
  {
    fprintf(logfile,"Error opening socket for IPC operations\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  if(setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
  {
    fprintf(logfile,"Error setting IPC socket option\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  host_addr.sin_family = AF_INET;
  host_addr.sin_port = htons(IPC_PORT);
  host_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(&(host_addr.sin_zero),'\0',8);

  if(bind(socketfd, (struct sockaddr *)&host_addr, sizeof(struct sockaddr)) == -1)
  {
    fprintf(logfile,"Error opening socket for IPC operations\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  if(listen(socketfd,1) == -1)
  {
    fprintf(logfile,"Error listening on IPC socket\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  while(1)
  {
    sin_size = sizeof(struct sockaddr_in);
    newsocket_fd = accept(socketfd, (struct sockaddr *)&client_addr, &sin_size);
    if(newsocket_fd == -1)
    {
      fprintf(logfile,"Error accepting IPC request\n");
      continue;
    }
    //acquire lock of shared data
    pthread_mutex_lock(&lock);

    recv_len = recv(newsocket_fd, ip_ad, 16, 0);
    while(recv_len > 0)
    {
      node_data* data = is_present(ip_ad);
      if(data == NULL) /*The data of requested node is not present*/
      {
        data = (node_data*)malloc(sizeof(node_data));
        data->core_count = 0;
        data->cpu_speed = 0;
        data->latency = 100000.0;
        data->bandwidth = 0;
        strncpy(data->ip_address, ip_ad, 16);
        data->next = NULL;
      }
      send(newsocket_fd, data, sizeof(node_data), 0);
      recv_len = recv(newsocket_fd, ip_ad, 16, 0);
    }
    close(newsocket_fd);
    //release the lock
    pthread_mutex_unlock(&lock);
  }
}

void* serve_rpc(void* data)
{
  fprintf(logfile,"RPC service started\n");
  struct rpc* data_t = (struct rpc*)data;
  int socketfd, newsocket_fd;
  struct sockaddr_in host_addr, client_addr;
  socklen_t sin_size;
  int recv_len=1, yes=1;
  char ip_ad[16];

  if((socketfd = socket(PF_INET,SOCK_STREAM,0)) == -1)
  {
    fprintf(logfile,"Error opening socket for RPC operations\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  if(setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
  {
    fprintf(logfile,"Error setting RPC socket option\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  host_addr.sin_family = AF_INET;
  host_addr.sin_port = htons(RPC_PORT);
  host_addr.sin_addr.s_addr = inet_addr(data_t->my_ip);
  memset(&(host_addr.sin_zero),'\0',8);

  if(bind(socketfd, (struct sockaddr *)&host_addr, sizeof(struct sockaddr)) == -1)
  {
    fprintf(logfile,"Error opening socket for RPC operations\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }

  if(listen(socketfd,1) == -1)
  {
    fprintf(logfile,"Error listening on RPC socket\n");
    fclose(logfile);
    exit(EXIT_FAILURE);
  }
  fprintf(logfile,"RPC Facilty on %s:%d\n",data_t->my_ip,RPC_PORT);
  while(1)
  {
    sin_size = sizeof(struct sockaddr_in);
    newsocket_fd = accept(socketfd, (struct sockaddr *)&client_addr, &sin_size);
    if(newsocket_fd == -1)
    {
      fprintf(logfile,"Error accepting RPC request\n");
      continue;
    }
    struct send_data* send_d = (struct send_data*)malloc(sizeof(struct send_data));
    send_d->core_count = get_core_count();
    send_d->cpu_speed = get_cpu_speed(data_t->my_ip, data_t->bench_dir);
    send(newsocket_fd, send_d, sizeof(struct send_data), 0);
    close(newsocket_fd);
  }
}

void benchmark_node(char* my_ip, node_data* curr_node,char* bench_dir, char* my_name)
{
  int sockfd, newsocket_fd;
  struct sockaddr_in client_addr;
  socklen_t sin_size;
  int recv_len=1;
  int yes = 1;

  if((sockfd = socket(PF_INET,SOCK_STREAM,0)) == -1)
  {
    fprintf(logfile,"Error opening socket for RPC operations for node %s\n",curr_node->ip_address);
    return;
  }

  if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
  {
    fprintf(logfile,"Error setting RPC socket option while benchmarking node %s\n",curr_node->ip_address);
    return;
  }

  client_addr.sin_family = AF_INET;
  client_addr.sin_port = htons(RPC_PORT);
  client_addr.sin_addr.s_addr = inet_addr(curr_node->ip_address);
  memset(&(client_addr.sin_zero),'\0',8);

  if(connect(sockfd, (struct sockaddr *)&client_addr, sizeof(struct sockaddr)) != 0)
  {
    fprintf(logfile,"Error connecting to remote host while benchmarking node %s\n",curr_node->ip_address);
    return;
  }
  struct send_data* host_data = (struct send_data*)malloc(sizeof(struct send_data*));
  if(host_data == NULL)
  {
    fprintf(logfile,"Error allocating memory while benchmarking node %s\n",curr_node->ip_address);
    return;
  }
  if(read(sockfd, host_data, sizeof(struct send_data)) == -1)
  {
    fprintf(logfile,"Error receiving data from host %s\n",curr_node->ip_address);
    close(sockfd);
    return;
  }
  close(sockfd);
  curr_node->cpu_speed = host_data->cpu_speed;
  curr_node->core_count = host_data->core_count;
  float latency_rec = get_latency(my_ip, curr_node->ip_address, bench_dir, my_name);
  if(latency_rec != -1)
  {
    curr_node->latency = latency_rec; /*Retain previous value in case of error*/
  }
  float bandwidth_rec = get_bandwidth(my_ip, curr_node->ip_address, bench_dir, my_name);
  if(bandwidth_rec != -1)
  {
    curr_node->bandwidth = bandwidth_rec; /*retain previous value in case of error*/
  }
}

void benchmark(char* fname, char* bench_dir)
{
  sleep(120); /*Wait at startup before first starting the benchmarking process*/
  while(1)
  {
    pthread_mutex_lock(&lock);
    FILE* file_handle = fopen(fname,"r");
    if(file_handle != NULL)
    {
      fprintf(logfile,"Error opening config file for benchmarking\n");
      exit(EXIT_FAILURE);
    }
    char my_ip[IPLEN];
    char host_ip[IPLEN];
    char my_name[MAX_HOST_NAME];
    fscanf(file_handle,"%s\n",my_name);
    fscanf(file_handle,"%s\n",my_ip);
    node_data* curr_node;
    if((curr_node = is_present(my_ip)) != NULL)
    {
      /*Update my info*/
      curr_node->core_count = get_core_count();
      curr_node->cpu_speed = get_cpu_speed(my_ip, bench_dir);
      curr_node->latency = 0;
      curr_node->bandwidth = 0;
    }
    else
    {
      /*Add a new node corresponding to this node along with its info*/
      node_data* new_node = (node_data*)malloc(sizeof(node_data));
      if(new_node == NULL)
      {
        fprintf(logfile,"Unable to allocate memory for new node\n");
        continue;
      }
      new_node->core_count = get_core_count();
      new_node->cpu_speed = get_cpu_speed(my_ip, bench_dir);
      new_node->latency = 0;
      new_node->bandwidth = 0;
      strncpy(new_node->ip_address, my_ip, 16);
      new_node->next = NULL;
      Add(new_node);
    }
    while(!feof(file_handle))
    {
      fscanf(file_handle,"%s\n",host_ip);
      if((curr_node = is_present(host_ip)) != NULL)
      {
        benchmark_node(my_ip, curr_node, bench_dir, my_name);
      }
      else
      {
        node_data* new_node = (node_data*)malloc(sizeof(node_data));
        if(new_node == NULL)
        {
          fprintf(logfile,"Error allocating memory for new node\n");
          continue;
        }
        new_node->next = NULL;
        /*Fill newly allocated node with some default values*/
        new_node->cpu_speed = 0;
        new_node->core_count = 0;
        new_node->latency = 100000.0;
        new_node->bandwidth = 0;
        strncpy(new_node->ip_address, host_ip, 16);
        benchmark_node(my_ip, new_node, bench_dir, my_name);
        Add(new_node);
      }
    }
    fclose(file_handle);
    pthread_mutex_unlock(&lock);
    sleep(900);
  }
}

int main(int argc, char** argv)   /* argv[1] is config file and argv[2] is the name of directory containing Intrl MPI Benchmarks*/
{
    /* Our process ID and Session ID */
    pid_t pid, sid;
    /* Fork off the parent process */
    pid = fork();
    if (pid < 0)
    {
      printf("Failed\n");
      exit(EXIT_FAILURE);
    }
    /* If we got a good PID, then
    we can exit the parent process. */
    if(pid > 0)
    {
      printf("The daemon is running with process ID : %d\n",pid);
      exit(EXIT_SUCCESS);
    }
    /* Change the file mode mask */
    umask(0);
    /* Create a new SID for the child process */
    sid = setsid();
    if (sid < 0)
    {
      exit(EXIT_FAILURE);
    }

    pthread_mutex_init(&lock,NULL); /*Initialize the mutex lock*/

    FILE* file_handle = fopen(argv[1],"r");
    if(file_handle == NULL)
    {
      printf("Unable to open config file to read data\n");
      exit(EXIT_FAILURE);
    }
    char my_ip[IPLEN];
    char my_name[MAX_HOST_NAME];
    fscanf(file_handle,"%s\n",my_name);
    fscanf(file_handle,"%s\n",my_ip);
    if(fclose(file_handle) != 0)
    {
      printf("Error closing config file\n");
    }
    char log[24];
    snprintf(log,24,"log_%s",my_ip);
    logfile = fopen(log,"w");

    /* Close out the standard file descriptors */
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    pthread_t thread1_id, thread2_id;
    if(pthread_create(&thread1_id,NULL,serve_ipc,NULL))
    {
      fprintf(logfile,"IPC Thread creation failed\n");
      exit(EXIT_FAILURE);
    }
    else
    {
      fprintf(logfile,"IPC Thread created successfully\n");
    }
    sleep(1);

    struct rpc* data = (struct rpc*)malloc(sizeof(struct rpc*));
    if(data == NULL)
    {
      fprintf(logfile,"Error allocating data for parameters to RPC server\n");
      exit(EXIT_FAILURE);
    }
    data->my_ip = my_ip;
    data->bench_dir = argv[2];
    if(pthread_create(&thread2_id,NULL,serve_rpc,data))
    {
      fprintf(logfile,"RPC Thread creation failed\n");
      exit(EXIT_FAILURE);
    }
    else
    {
      fprintf(logfile,"RPC Thread creation successfully\n");
    }
    benchmark(argv[1],argv[2]); /*Benchmark various nodes*/

    exit(EXIT_SUCCESS);
}
