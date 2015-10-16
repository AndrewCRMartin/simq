/************************************************************************/
/**

   Program:    simq
   \file       simq.c
   
   \version    V1.0 
   \date       16.10.15   
   \brief      A very simple batch queuing program
   
   \copyright  (c) UCL / Dr. Andrew C. R. Martin 2015
   \author     Dr. Andrew C. R. Martin
   \par
               Institute of Structural & Molecular Biology,
               University College,
               Gower Street,
               London.
               WC1E 6BT.
   \par
               andrew@bioinf.org.uk
               andrew.martin@ucl.ac.uk
               
**************************************************************************

   This program is not in the public domain, but it may be copied
   according to the conditions laid out in the accompanying file
   COPYING.DOC

   The code may be modified as required, but any modifications must be
   documented so that the person responsible can be identified.

   The code may not be sold commercially or included as part of a 
   commercial product except as described in the file COPYING.DOC.

**************************************************************************

   Description:
   ============

**************************************************************************

   Usage:
   ======

**************************************************************************

   Revision History:
   =================
-  V1.0    16.10.15  Original   By: ACRM

*************************************************************************/
/* Includes
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pwd.h>

/************************************************************************/
/* Defines and macros
*/
#define PROGNAME "simq"
#define MAXBUFF 240
#define LOCKFILE ".lock"
#define JOB_NEWEST 1
#define JOB_OLDEST 2
#define MSG_INFO    0
#define MSG_WARNING 1
#define MSG_ERROR   2
#define MSG_FATAL   3
#define DEF_POLLTIME 10
#define DEF_WAITTIME 60

typedef short BOOL;
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#define CLOSE_AND_RETURN(fp, pid)                              \
   { fprintf(stderr,"Warning: Invalid job file (%d)\n", pid);  \
     fclose(fp);                                               \
     return;                                                   \
   }                                                           \

/* From bioplib/macros.h */
#define TERMINATE(x) do {  int _terminate_macro_j;              \
                        for(_terminate_macro_j=0;               \
                            (x)[_terminate_macro_j];            \
                            _terminate_macro_j++)               \
                        {  if((x)[_terminate_macro_j] == '\n')  \
                           {  (x)[_terminate_macro_j] = '\0';   \
                              break;                            \
                     }  }  }  while(0)
      

/************************************************************************/
/* Globals
*/

/************************************************************************/
/* Prototypes
*/

BOOL ParseCmdLine(int argc, char **argv, BOOL *run, int *progArg, int *sleepTime, int *verbose, char *queueDir, int *maxWait);
int main(int argc, char **argv);
void MakeDirectory(char *dirname);
void UsageDie(void);
int QueueJob(char *queueDir, char *lockFullFile, char **progArgs, int nProgArgs, int maxWait);
void SpawnJobRunner(char *queueDir, int sleepTime, int verbose);
BOOL RunNextJob(char *queueDir, int verbose);
void RunJob(char *queueDir, int pid, int verbose);
int FindJobs(char *queueDir, int oldNew, int *pid);
int FlockFile(char *filename);
void FunlockFile(int fh, char *lockFileFull);
void WriteJobFile(char *queueDir, int pid, char **progArgs, int nProgArgs);
BOOL FileExists(char *filename);
BOOL IsRootUser(uid_t *uid, gid_t *gid);
void Message(char *progname, int level, char *message);

/************************************************************************/
int main(int argc, char **argv)
{
    BOOL run = FALSE;
    int  progArg = (-1);
    int  sleepTime = DEF_POLLTIME;
    int  verbose   = 0;
    int  maxWait   = DEF_WAITTIME;
    char queueDir[MAXBUFF];
    uid_t uid;
    gid_t gid;
    
    

    if(ParseCmdLine(argc, argv, &run, &progArg, &sleepTime, &verbose, queueDir, &maxWait))
    {
       char lockFullFile[MAXBUFF];
                                  
       sprintf(lockFullFile, "%s/%s", queueDir, LOCKFILE);
       MakeDirectory(queueDir);

       if(run)
       {
          if(!IsRootUser(&uid, &gid))
          {
             Message(PROGNAME, MSG_FATAL, "With -run, the program must be run as root.");
          }
          unlink(lockFullFile);
          SpawnJobRunner(queueDir, sleepTime, verbose);
       }
       else
       {
          int nJobs;
          if(IsRootUser(&uid, &gid))
          {
             Message(PROGNAME, MSG_FATAL, "Jobs may not be submitted by root");
          }
          
          nJobs = QueueJob(queueDir, lockFullFile, argv+progArg, argc-progArg, maxWait);
          if(verbose)
          {
             fprintf(stderr,"There are now %d jobs in the queue\n", nJobs);
          }
       }
    }
    else
    {
        UsageDie();
    }
    return(0);
}

/************************************************************************/
BOOL ParseCmdLine(int argc, char **argv, BOOL *run, int *progArg, int *sleepTime, int *verbose, char *queueDir, int *maxWait)
{
    argc--;
    argv++;

    *progArg = 1;
    queueDir[0] = '\0';

    while(argc && (argv[0][0] == '-'))
    {
        switch(argv[0][1])
        {
        case 'h':
           return(FALSE);
           break;
        case 'r':
           *run = TRUE;
           break;
        case 'p':
           argc--;
           argv++;
           (*progArg)++;
           if(!argc || !sscanf(argv[0], "%d", sleepTime))
              return(FALSE);
           break;
        case 'w':
           argc--;
           argv++;
           (*progArg)++;
           if(!argc || !sscanf(argv[0], "%d", maxWait))
              return(FALSE);
           break;
        case 'v':
           *verbose = strlen(argv[0]) - 1;
           break;
        }
        argc--;
        argv++;
        (*progArg)++;
    }
    
    if(*run)
    {
       if(argc != 1)
          return(FALSE);
    }
    else
    {
       if(argc < 2)
          return(FALSE);
    }

    strncpy(queueDir, argv[0], MAXBUFF);
    argc--;
    argv++;
    (*progArg)++;

    return(TRUE);
}


/************************************************************************/
void MakeDirectory(char *dirname)
{
   mode_t mode = 0777;
   
   mkdir(dirname, mode);
}



/************************************************************************/
int QueueJob(char *queueDir, char *lockFullFile, char **progArgs, int nProgArgs, int maxWait)
{
   int pid = 1;                /* Default PID */
   int fh;
   int nJobs;
   int waitCount = 0;
   
   
   /* Wait while the lock file is present */
   while(FileExists(lockFullFile))
   { 
      if(++waitCount > maxWait)
      {
         Message(PROGNAME, MSG_FATAL, "Cannot submit job - lockfile is not clearing");
      }
      
      sleep(1);

   }
   
   /* Create lock file */
   if((fh = FlockFile(lockFullFile)) == (-1))
   {
      Message(PROGNAME, MSG_FATAL, "Cannot create lock file");
   }
   
   
   /* Find the latest job and number of jobs queued */
   nJobs = FindJobs(queueDir, JOB_NEWEST, &pid);
   if(nJobs) pid++;
   
   /* Write the job file */
   WriteJobFile(queueDir, pid, progArgs, nProgArgs);
   
   /* Remove the lock file */
   FunlockFile(fh, lockFullFile);
   
   return(nJobs+1);
}

/************************************************************************/
void SpawnJobRunner(char *queueDir, int sleepTime, int verbose)
{
   while(1)
   {
      if(!RunNextJob(queueDir, verbose))
      {
         sleep(sleepTime);
      }
   }
}

/************************************************************************/
BOOL RunNextJob(char *queueDir, int verbose)
{
   BOOL retVal = FALSE;
   int nJobs;
   int pid;

   /* List the directory */
   nJobs = FindJobs(queueDir, JOB_OLDEST, &pid);

   if(nJobs)
   {
      /* Run the job */
      RunJob(queueDir, pid, verbose);
      retVal = TRUE;
   }
   else if(verbose >= 2)
   {
      fprintf(stderr, "Info: No jobs waiting\n");
   }
   return(retVal);
}

/************************************************************************/
void RunJob(char *queueDir, int pid, int verbose)
{
   char jobFile[MAXBUFF];
   FILE *fp;
   char pwd[MAXBUFF],
      job[MAXBUFF],
      exe[MAXBUFF],
      cmd[MAXBUFF];
   uid_t uid;
   struct stat statBuff;
   

   sprintf(jobFile, "%s/%d", queueDir, pid);

   if(verbose)
      fprintf(stderr, "Info: Running job %d\n", pid);

   if((fp=fopen(jobFile, "r"))!=NULL)
   {
      char          *username;
      struct passwd *pwdBuff;
      
      /* Find the ownder of the job file */
      stat(jobFile, &statBuff);
      uid = statBuff.st_uid;

      /* Get the username from the UID                                  */
      pwdBuff  = getpwuid(uid);
      username = pwdBuff->pw_name;
      
      /* Get the working directory for running the job */
      if(!fgets(pwd, MAXBUFF, fp))
         CLOSE_AND_RETURN(fp, pid);
      TERMINATE(pwd);

      /* Get the job itself */
      if(!fgets(job, MAXBUFF, fp))
         CLOSE_AND_RETURN(fp, pid);
      TERMINATE(job);

      fclose(fp);

      /* Run the job as the requested user                              */
      sprintf(cmd, "(cd %s; %s)", pwd, job);
      sprintf(exe, "su - %s -c \"%s\"", username, cmd);

/*      sprintf(exe, "(cd %s; %s)", pwd, job); */

      if(verbose >= 2)
         fprintf(stderr, "Info: Command is: %s\n", cmd);
      if(verbose >= 3)
         fprintf(stderr, "Info: Expanded command is: %s\n", exe);

      system(exe);

      /* Remove the job from the queue */
      unlink(jobFile);
   }
}




/************************************************************************/
int FindJobs(char *queueDir, int oldNew, int *pid)
{
   struct dirent *dirp;
   DIR           *dp;
   int           thisPID,
                 nJobs = 0,
                 newest = (-1),
                 oldest = (-1);

   if((dp=opendir(queueDir)) == NULL)
   {
      fprintf(stderr,"Error: Can't read directory: %s\n", queueDir);
      exit(1);
   }

   while((dirp = readdir(dp)) != NULL)
   {
      /* Ignore files starting with a .                                 */
      if(dirp->d_name[0] != '.')
      {
         /* Check it's a number */
         if(sscanf(dirp->d_name, "%d", &thisPID))
         {
            /* Initialize */
            if(newest == (-1))
            {
               newest    = thisPID;
            }
            
            if(oldest == (-1))
            { 
               oldest    = thisPID;
            }

            /* Update */
            if(thisPID > newest)
            {
               newest    = thisPID;
            }
            
            if(thisPID < oldest)
            { 
               oldest    = thisPID;
            }

            nJobs++;
         }
      }
   }
   
   closedir(dp);

   if(nJobs)
   {
      if(oldNew == JOB_NEWEST)
      {
         *pid = newest;
      }
      else
      {
         *pid = oldest;
      }
   }

   return(nJobs);
}


/************************************************************************/
int FlockFile(char *filename)
{
   int fh = 0;
   
   if((fh = open(filename, O_WRONLY|O_CREAT|O_APPEND))!=(-1))
   {
      if(flock(fh, LOCK_EX) == (0))
      {
         return(fh);
      }
   }
   Message(PROGNAME, MSG_FATAL, "Cannot create lock file");
   return(-1);
}

/************************************************************************/
void FunlockFile(int fh, char *lockFileFull)
{
    close(fh);
    unlink(lockFileFull);
}

/************************************************************************/
void WriteJobFile(char *queueDir, int pid, char **progArgs, int nProgArgs)

{
   char jobFile[MAXBUFF],
        pwd[MAXBUFF];
   FILE *fp;


   sprintf(jobFile, "%s/%d", queueDir, pid);
   getcwd(pwd, MAXBUFF);
   TERMINATE(pwd);

   if((fp=fopen(jobFile, "w"))!=NULL)
   {
      int i;
      fprintf(fp, "%s\n", pwd);
      for(i=0; i<nProgArgs; i++)
      {
         fprintf(fp, "%s ", progArgs[i]);
      }
      fprintf(fp, "\n");
      fclose(fp);
   }
   else
   {
      fprintf(stderr,"Error: unable to create job file %s\n", jobFile);
   }
}

/************************************************************************/
BOOL FileExists(char *filename)
{
   if(access(filename, F_OK) != 0)
      return(FALSE);
   return(TRUE);

}

/************************************************************************/
BOOL IsRootUser(uid_t *uid, gid_t *gid)
{
   *uid = getuid();
   *gid = getgid();
   
   if((*uid == (uid_t)0) || (*gid == (gid_t)0))
   {
      return(TRUE);
   }
   return(FALSE);
}

/************************************************************************/
void Message(char *progname, int level, char *message)
{
   switch(level)
   {
   case MSG_INFO:
      fprintf(stderr, "Info ");
      break;
   case MSG_WARNING:
      fprintf(stderr, "Warning ");
      break;
   case MSG_ERROR:
   case MSG_FATAL:
      fprintf(stderr, "Error ");
      break;
   }
   fprintf(stderr, "(%s) %s\n", progname, message);
   
   if(level == MSG_FATAL)
      exit(1);
}


/************************************************************************/
void UsageDie(void)
{
   fprintf(stderr,"%s V1.0 (c) 2015 UCL, Dr. Andrew C.R. Martin\n", PROGNAME);
   fprintf(stderr,"\n");
   fprintf(stderr,"Usage:   %s [-v[v...]] [-p polltime] -run queuedir\n", PROGNAME);
   fprintf(stderr,"         %s [-v[v...]] [-w maxwait] queuedir program [parameters ...]\n", PROGNAME);
   fprintf(stderr,"         -v Verbose mode (-vv, -vvv more info)\n");
   fprintf(stderr,"         -p Specify the wait in seconds between polling for jobs [%d]\n",  DEF_POLLTIME);
   fprintf(stderr,"         -w Specify maximum wait time when trying to submit a job [%d]\n", DEF_WAITTIME);
   fprintf(stderr,"         -run Run in daemon mode to wait for jobs\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"%s is a simple program batch queueing system. It simply\n", PROGNAME);
   fprintf(stderr,"places jobs in a queue by placing the command in a file that lives in\n");
   fprintf(stderr,"the 'queuedir' directory. The queue manager then pulls jobs off one at\n");
   fprintf(stderr,"a time and runs them.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"It should be run first and backgrounded with the -run flag. e.g.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"    nohup nice -10 %s -run /var/tmp/queue1 &\n", PROGNAME);
   fprintf(stderr,"\n");
   fprintf(stderr,"The queue directory ('queuedir') will be created if it does not exist,\n");
   fprintf(stderr,"but you may need to change the permissions to allow the person\n");
   fprintf(stderr,"submitting a job (perhaps apache) to write to that directory.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"*** Warning! Jobs will be run by the user who has started ***\n");
   fprintf(stderr,"*** queueprogram with the -run flag, not by the submitter ***\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"To submit a job, run queueprogram as, for example:\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"   %s /var/tmp/queue1 myprogram param1 param2 \n", PROGNAME);
   fprintf(stderr,"\n");

   exit(0);
}

