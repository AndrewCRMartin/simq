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

#define CLOSE_AND_RETURN(fp, jobID)                                      \
   {  char car_msg[MAXBUFF];                                             \
      sprintf(car_msg,"Invalid Job file (%d)", jobID);                   \
      Message(PROGNAME, MSG_WARNING, car_msg);                           \
      fclose(fp);                                                        \
      return;                                                            \
   }

/* From bioplib/macros.h                                                */
#define TERMINATE(x) do {  int _terminate_macro_j;                       \
                        for(_terminate_macro_j=0;                        \
                            (x)[_terminate_macro_j];                     \
                            _terminate_macro_j++)                        \
                        {  if((x)[_terminate_macro_j] == '\n')           \
                           {  (x)[_terminate_macro_j] = '\0';            \
                              break;                                     \
                     }  }  }  while(0)


/************************************************************************/
/* Globals
*/

/************************************************************************/
/* Prototypes
*/
BOOL ParseCmdLine(int argc, char **argv, BOOL *runDaemon, int *progArg, 
                  int *sleepTime, int *verbose, char *queueDir,
                  int *maxWait, BOOL *listJobs);
int main(int argc, char **argv);
void MakeDirectory(char *dirname);
void UsageDie(void);
int QueueJob(char *queueDir, char *lockFullFile, char **progArgs, 
             int nProgArgs, int maxWait);
void SpawnJobRunner(char *queueDir, int sleepTime, int verbose);
BOOL RunNextJob(char *queueDir, int verbose);
void RunJob(char *queueDir, int jobID, int verbose);
int FindJobs(char *queueDir, int oldNew, int *jobID);
int FlockFile(char *filename);
void FunlockFile(int fh, char *lockFileFull);
void WriteJobFile(char *queueDir, int jobID, char **progArgs, 
                  int nProgArgs);
BOOL FileExists(char *filename);
BOOL IsRootUser(uid_t *uid, gid_t *gid);
void Message(char *progname, int level, char *message);
void ListJobs(char *queueDir, int verbose);


/************************************************************************/
/*>int main(int argc, char **argv)
   -------------------------------
*//**
   Main program

   - 16.10.15   Original   By: ACRM
*/
int main(int argc, char **argv)
{
   BOOL  runDaemon = FALSE, 
         listJobs  = FALSE;
   int   progArg   = (-1),
         verbose   = 0,
         sleepTime = DEF_POLLTIME,
         maxWait   = DEF_WAITTIME;
   char  queueDir[MAXBUFF];
   uid_t uid;
   gid_t gid;
    
   if(ParseCmdLine(argc, argv, &runDaemon, &progArg, &sleepTime, 
                   &verbose, queueDir, &maxWait, &listJobs))
   {
      char lockFullFile[MAXBUFF];
      
      sprintf(lockFullFile, "%s/%s", queueDir, LOCKFILE);
      MakeDirectory(queueDir);
      
      if(runDaemon)
      {
         if(!IsRootUser(&uid, &gid))
         {
            Message(PROGNAME, MSG_FATAL, 
                    "With -run, the program must be run as root.");
         }
         unlink(lockFullFile);
         SpawnJobRunner(queueDir, sleepTime, verbose);
      }
      else if (listJobs)
      {
         ListJobs(queueDir, verbose);
      }
      else
      {
         int nJobs;
         if(IsRootUser(&uid, &gid))
         {
            Message(PROGNAME, MSG_FATAL,
                    "Jobs may not be submitted by root");
         }
         
         nJobs = QueueJob(queueDir, lockFullFile, argv+progArg, 
                          argc-progArg, maxWait);
         if(verbose)
         {
            char msg[MAXBUFF];
            sprintf(msg, "There are now %d jobs in the queue", nJobs);
            Message(PROGNAME, MSG_INFO, msg);
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
/*>BOOL ParseCmdLine(int argc, char **argv, BOOL *runDaemon, int *progArg,
                     int *sleepTime, int *verbose, char *queueDir, 
                     int *maxWait, BOOL *listJobs)
   -----------------------------------------------------------------
*//**
   \param[in]  argc          Argument count
   \param[in]  **argv        Argument array
   \param[out] *runDaemon    Run mode specified
   \param[out] *progArg      offset into argv of the program to run
   \param[out] *sleepTime    how long to wait between polls for jobs
   \param[out] *verbose      verbose information
   \param[out] *queueDir     the queue directory
   \param[out] *maxWait      maximum time to wait when submitting job
   \param[out] *listJobs     List the waiting jobs
   \returns                  OK

   Parses the command line

-  16.10.15  Original   By: ACRM
*/
BOOL ParseCmdLine(int argc, char **argv, BOOL *runDaemon, int *progArg, 
                  int *sleepTime, int *verbose, char *queueDir, 
                  int *maxWait, BOOL *listJobs)
{
    argc--;
    argv++;

    *progArg    = 1;
    queueDir[0] = '\0';

    while(argc && (argv[0][0] == '-'))
    {
        switch(argv[0][1])
        {
        case 'h':
           return(FALSE);
           break;
        case 'r':
           *runDaemon = TRUE;
           break;
        case 'l':
           *listJobs = TRUE;
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
    
    if(*runDaemon || *listJobs)
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

    if(queueDir[0] != '/')
       return(FALSE);

    return(TRUE);
}


/************************************************************************/
/*>void MakeDirectory(char *dirname)
   ---------------------------------
*//**
   \param[in] dirname    Directory name to be created

   Creates a directory if it doesn't exist and sets the permissions
   as required.

-  16.10.15  Original   By: ACRM
*/
void MakeDirectory(char *dirname)
{
   /*** TODO Set permissions correctly ***/
   mode_t mode = 01777;

   /* If directory doesn't exist                                        */
   if(access(dirname, F_OK) != 0)
   {
      mode_t oldMask;
      
      oldMask = umask((mode_t)0000);
      if(mkdir(dirname, mode) != 0)
      {
         Message(PROGNAME, MSG_FATAL, "Could not create queue directory");
      }
      umask(oldMask);
   }
}


/************************************************************************/
/*>int QueueJob(char *queueDir, char *lockFullFile, char **progArgs,
                int nProgArgs, int maxWait)
   -----------------------------------------------------------------
*//**
   \param[in]  *queueDir      The queue directory
   \param[in]  *lockFullFile  Full path and name of lock file
   \param[in]  **progArgs     The program name and arguments
   \param[in]  nProgArgs      The size of the arguments array
   \param[in]  maxWait        Max time to wait to create a job
   \return                    Number of jobs in the queue

   Adds a job to the queue and returns the number of jobs now in the 
   queue.

-  16.10.15  Original   By: ACRM
*/
int QueueJob(char *queueDir, char *lockFullFile, char **progArgs,
             int nProgArgs, int maxWait)
{
   int jobID     = 1,          /* Default JOBID                         */
       waitCount = 0,
       fh,
       nJobs;
   
   
   /* Wait while the lock file is present                               */
   while(FileExists(lockFullFile))
   { 
      if(++waitCount > maxWait)
      {
         Message(PROGNAME, MSG_FATAL, 
                 "Cannot submit job - lockfile is not clearing");
      }
      
      sleep(1);
   }
   
   /* Create lock file                                                  */
   if((fh = FlockFile(lockFullFile)) == (-1))
   {
      Message(PROGNAME, MSG_FATAL, "Cannot create lock file");
   }
   
   /* Find the latest job and number of jobs queued                     */
   nJobs = FindJobs(queueDir, JOB_NEWEST, &jobID);
   if(nJobs) jobID++;
   
   /* Write the job file                                                */
   WriteJobFile(queueDir, jobID, progArgs, nProgArgs);
   
   /* Remove the lock file                                              */
   FunlockFile(fh, lockFullFile);
   
   return(nJobs+1);
}


/************************************************************************/
/*>void SpawnJobRunner(char *queueDir, int sleepTime, int verbose)
   ---------------------------------------------------------------
*//**
   \param[in]  queueDir   The queue directory
   \param[in]  sleepTime  Time to wait between polling for jobs
   \param[in]  verbose    Verbosity level

   Sits waiting for jobs and runs them when one appears

-  16.10.15  Original   By: ACRM
*/
void SpawnJobRunner(char *queueDir, int sleepTime, int verbose)
{
   /*** Ideally this should detach itself in the background ***/

   while(1)
   {
      if(!RunNextJob(queueDir, verbose))
      {
         sleep(sleepTime);
      }
   }
}


/************************************************************************/
/*>BOOL RunNextJob(char *queueDir, int verbose)
   --------------------------------------------
*//**
   \param[in]  queueDir   The queue directory
   \param[in]  verbose    Verbosty level
   \return                Were there any jobs to run?

   Find the next job in the queue and run it

-  16.10.15  Original   By: ACRM
*/
BOOL RunNextJob(char *queueDir, int verbose)
{
   BOOL retVal = FALSE;
   int  nJobs,
        jobID;

   /* List the directory                                                */
   nJobs = FindJobs(queueDir, JOB_OLDEST, &jobID);

   if(nJobs)
   {
      /* Run the job                                                    */
      RunJob(queueDir, jobID, verbose);
      retVal = TRUE;
   }
   else if(verbose >= 2)
   {
      Message(PROGNAME, MSG_INFO, "No jobs waiting");
   }
   return(retVal);
}


/************************************************************************/
/*>void RunJob(char *queueDir, int jobID, int verbose)
   ---------------------------------------------------
*//**
   \param[in]  queueDir   Queue directory
   \param[in]  jobID      Job number
   \param[in]  verbose    Verbosity level

   Actually runs a job

-  16.10.15  Original   By: ACRM
*/
void RunJob(char *queueDir, int jobID, int verbose)
{
   struct stat statBuff;
   char        jobFile[MAXBUFF];
   FILE        *fp;
   char        pwd[MAXBUFF],
               job[MAXBUFF],
               exe[MAXBUFF],
               cmd[MAXBUFF];
   uid_t       uid;
   

   sprintf(jobFile, "%s/%d", queueDir, jobID);

   if(verbose)
   {
      char msg[MAXBUFF];
      sprintf(msg, "Running job %d", jobID);
      Message(PROGNAME, MSG_INFO, msg);
   }

   if((fp=fopen(jobFile, "r"))!=NULL)
   {
      char          *username;
      struct passwd *pwdBuff;
      
      /* Find the ownder of the job file                                */
      stat(jobFile, &statBuff);
      uid = statBuff.st_uid;

      /* Get the username from the UID                                  */
      pwdBuff  = getpwuid(uid);
      username = pwdBuff->pw_name;
      
      /* Get the working directory for running the job                  */
      if(!fgets(pwd, MAXBUFF, fp))
         CLOSE_AND_RETURN(fp, jobID);
      TERMINATE(pwd);

      /* Get the job itself                                             */
      if(!fgets(job, MAXBUFF, fp))
         CLOSE_AND_RETURN(fp, jobID);
      TERMINATE(job);

      fclose(fp);

      /* Run the job as the requested user                              */
      sprintf(cmd, "(cd %s; %s)", pwd, job);
      sprintf(exe, "su - %s -c \"%s\"", username, cmd);

      if(verbose >= 2)
      {
         char msg[MAXBUFF];
         sprintf(msg, "Command is: %s", cmd);         
         Message(PROGNAME, MSG_INFO, msg);
      }
      
      if(verbose >= 3)
      {
         char msg[MAXBUFF];
         sprintf(msg, "Expanded command is: %s", exe);         
         Message(PROGNAME, MSG_INFO, msg);
      }
      
      system(exe);

      /* Remove the job from the queue                                  */
      unlink(jobFile);
   }
}


/************************************************************************/
/*>int FindJobs(char *queueDir, int oldNew, int *jobID)
   ----------------------------------------------------
*//**
   \param[in]  queueDir   Queue directory
   \param[in]  oldNew     Are we looking for the oldest or newest job
   \param[in]  *jobID     Job number of the job we found (-1 if no jobs)
   \return                Number of queued jobs

   Finds the oldest or newest job in the queue. Sets the job ID for that
   job and also returns the number of jobs in the queue.

-  16.10.15  Original   By: ACRM
*/
int FindJobs(char *queueDir, int oldNew, int *jobID)
{
   struct dirent *dirp;
   DIR           *dp;
   int           thisJobID,
                 nJobs       = 0,
                 newestJobID = (-1),
                 oldestJobID = (-1);


   if((dp=opendir(queueDir)) == NULL)
   {
      char msg[MAXBUFF];
      sprintf(msg, "Can't read directory: %s", queueDir);
      Message(PROGNAME, MSG_FATAL, msg);
   }

   while((dirp = readdir(dp)) != NULL)
   {
      /* Ignore files starting with a .                                 */
      if(dirp->d_name[0] != '.')
      {
         /* Check it's a number                                         */
         if(sscanf(dirp->d_name, "%d", &thisJobID))
         {
            /* Initialize                                               */
            if(newestJobID == (-1))   newestJobID = thisJobID;
            if(oldestJobID == (-1))   oldestJobID = thisJobID;

            /* Update                                                   */
            if(thisJobID > newestJobID) newestJobID = thisJobID;
            if(thisJobID < oldestJobID) oldestJobID = thisJobID;

            nJobs++;
         }
      }
   }
   
   closedir(dp);

   if(nJobs)
   {
      if(oldNew == JOB_NEWEST)
      {
         *jobID = newestJobID;
      }
      else
      {
         *jobID = oldestJobID;
      }
   }

   return(nJobs);
}


/************************************************************************/
/*>int FlockFile(char *filename)
   -----------------------------
*//**
   \param[in]  *filename   File name
   \return                 File handle

   Creates a lock file

-  16.10.15  Original   By: ACRM
*/
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
/*>void FunlockFile(int fh, char *lockFileFull)
   --------------------------------------------
*//**
   \param[in]  fh              File handle of lock file
   \param[in]  *lockFileFull   Full path to lock file

   Closes and deletes a lock file

-  16.10.15  Original   By: ACRM
*/
void FunlockFile(int fh, char *lockFileFull)
{
    close(fh);
    unlink(lockFileFull);
}

/************************************************************************/
/*>void WriteJobFile(char *queueDir, int pid, char **progArgs, 
                     int nProgArgs)
   -----------------------------------------------------------
*//**
   \param[in]  *queueDir    queue directory
   \param[in]  pid          job number
   \param[in]  **progArgs   Program and arguments in an array
   \param[in]  nProgArgs    Number of items in progArgs

   Creates a job file

-  16.10.15  Original   By: ACRM
*/
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
      char msg[MAXBUFF];
      sprintf(msg,"Unable to create job file %s", jobFile);
      Message(PROGNAME, MSG_FATAL, msg);
   }
}


/************************************************************************/
/*>BOOL FileExists(char *filename)
   -------------------------------
*//**
   \param[in]  filename   File name
   \return                Does the file exist

   Tests if a file exists

-  16.10.15  Original   By: ACRM
*/
BOOL FileExists(char *filename)
{
   if(access(filename, F_OK) != 0)
      return(FALSE);
   return(TRUE);

}


/************************************************************************/
/*>BOOL IsRootUser(uid_t *uid, gid_t *gid)
   ---------------------------------------
*//**
   \param[out]  *uid   User ID
   \param[out]  *gid   Group ID
   \return             True if root, False otherwise

   Tests if this program is being run by root. Also finds the user id
   and group id of the user.

-  16.10.15  Original   By: ACRM
*/
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
/*>void Message(char *progname, int level, char *message)
   ------------------------------------------------------
*//**
   \param[in]  *progname   Program name
   \param[in]  level       Message severity (MSG_INFO, MSG_WARNING,
                           MSG_ERROR, MSG_FATAL)
   \param[in]  message     Message to display

   Prints a message and, if the severity is MSG_FATAL, exist the program

-  16.10.15  Original   By: ACRM
*/
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
void ListJobs(char *queueDir, int verbose)
{
   struct dirent *dirp;
   DIR           *dp;
   int           nJobs       = 0;

   if((dp=opendir(queueDir)) == NULL)
   {
      char msg[MAXBUFF];
      sprintf(msg, "Can't read directory: %s", queueDir);
      Message(PROGNAME, MSG_FATAL, msg);
   }

   while((dirp = readdir(dp)) != NULL)
   {
      int thisJobID;
      
      /* Ignore files starting with a .                                 */
      if(dirp->d_name[0] != '.')
      {
         /* Check it's a number                                         */
         if(sscanf(dirp->d_name, "%d", &thisJobID))
         {
            nJobs++;
         }
      }
   }
   
   closedir(dp);

   printf("Jobs waiting: %d\n", nJobs);
}


/************************************************************************/
/*>void UsageDie(void)
   -------------------
*//**
   Prints a usage message

-  16.10.15  Original   By: ACRM
*/
void UsageDie(void)
{
   fprintf(stderr,"\n%s V1.0 (c) 2015 UCL, Dr. Andrew C.R. Martin\n", 
           PROGNAME);
   fprintf(stderr,"\n");
   fprintf(stderr,"Usage:   %s [-v[v...]] [-p polltime] -run queuedir\n",
           PROGNAME);
   fprintf(stderr,"         %s [-v[v...]] [-w maxwait] queuedir program \
[parameters ...]\n", PROGNAME);
   fprintf(stderr,"         %s -l queuedir\n", PROGNAME);
   fprintf(stderr,"         -v   Verbose mode (-vv, -vvv more info)\n");
   fprintf(stderr,"         -p   Specify the wait in seconds between \
polling for jobs [%d]\n",  DEF_POLLTIME);
   fprintf(stderr,"         -w   Specify maximum wait time when trying \
to submit a job [%d]\n", DEF_WAITTIME);
   fprintf(stderr,"         -l   List number of waiting jobs\n");
   fprintf(stderr,"         -run Run in daemon mode to wait for jobs\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"%s is a simple program batch queueing system. It \
simply\n", PROGNAME);
   fprintf(stderr,"places jobs in a queue by placing the command in a \
file that lives in\n");
   fprintf(stderr,"the 'queuedir' directory. The queue manager then \
pulls jobs off one at\n");
   fprintf(stderr,"a time and runs them.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"Note that 'queuedir' must be a full path name.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"It should be run first and backgrounded with the \
-run flag. e.g.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"    nohup nice -10 %s -run /var/tmp/queue1 &\n", 
           PROGNAME);
   fprintf(stderr,"\n");
   fprintf(stderr,"The queue directory ('queuedir') will be created if \
it does not exist,\n");
   fprintf(stderr,"but you may need to change the permissions to allow \
the person\n");
   fprintf(stderr,"submitting a job (perhaps apache) to write to that \
directory.\n");
   fprintf(stderr,"\n");
   fprintf(stderr,"To submit a job, run %s as, for example:\n", PROGNAME);
   fprintf(stderr,"\n");
   fprintf(stderr,"   %s /var/tmp/queue1 myprogram param1 param2 \n",
           PROGNAME);
   fprintf(stderr,"\n");

   exit(0);
}

