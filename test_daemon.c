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

int main()
{
  struct sockaddr_in address;
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char *hello1 = "172.27.19.21";
  char *hello2 = "172.27.19.24";
  node_data* buffer = (node_data*)malloc(sizeof(node_data));
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
      printf("\n Socket creation error \n");
      return -1;
  }

  memset(&serv_addr, '0', sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(IPC_PORT);

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)
  {
      printf("\nInvalid address/ Address not supported \n");
      return -1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
      printf("\nConnection Failed \n");
      return -1;
  }
  for(int i=0;i<2;i++)
  {
    if(i==0)
{
    send(sock , hello1 , strlen(hello1) , 0 );
    printf("Data of node %s\n",hello1);
    valread = read( sock , buffer, sizeof(node_data));
    printf("%d\n",buffer->core_count);
    printf("%f\n",buffer->cpu_speed);
    printf("%f\n",buffer->latency);
    printf("%f\n",buffer->bandwidth);
}
else
{
    send(sock,hello2,strlen(hello2),0);
    printf("Data of node %s\n",hello2);
    valread=read(sock,buffer,sizeof(node_data));
    printf("%d\n",buffer->core_count);
    printf("%f\n",buffer->cpu_speed);
    printf("%f\n",buffer->latency);
    printf("%f\n",buffer->bandwidth);
}

  }
  //printf("%s\n",buffer );
  return 0;
}
