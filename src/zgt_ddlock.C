/**
 *  CSE 5331     : DBMS Models and implementation
 *  Project 1    : Transaction Manager
 *  Team Members : Anvit Bhimsain Joshi (1001163195) and Rajat Dhanuka (1001214104)
 */

/*------------------------------------------------------------------------------
//                         RESTRICTED RIGHTS LEGEND
//
// Use,  duplication, or  disclosure  by  the  Government is subject 
// to restrictions as set forth in subdivision (c)(1)(ii) of the Rights
// in Technical Data and Computer Software clause at 52.227-7013. 
//
// Copyright 1989, 1990, 1991 Texas Instruments Incorporated.  All rights reserved.
//------------------------------------------------------------------------------
 */

#define NULL	0
#define TRUE	1
#define FALSE	0
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"

extern zgt_tm *ZGT_Sh;

/*wait_for constructor
 * Initializes the waiting transaction in wtable
 * and setting the appropriate pointers.
 */
wait_for::wait_for() {
    zgt_tx *tp;
    node* np;
    wtable = NULL;
    fprintf(stdout, "::entering wait_for method:::\n");
    fflush(stdout);
    for (tp = (zgt_tx *) ZGT_Sh->lastr; tp != NULL; tp = tp->nextr) {
        if (tp->status != TR_WAIT) continue;
        np = new node;
        np->tid = tp->tid;
        np->sgno = tp->sgno;
        np->obno = tp->obno;
        np->lockmode = tp->lockmode;
        np->semno = tp->semno;
        np->next = (wtable != NULL) ? wtable : NULL;
        np->parent = NULL;
        np->next_s = NULL;
        np->level = 0;
        wtable = np;
        printf("\nwtable****%d %d %d %c %d %d\n", wtable->tid, wtable->sgno, wtable->obno, wtable->lockmode, wtable->semno, wtable->level);
    }
    fprintf(stdout, "::leaving wait_for method:::\n\n");
    fflush(stdout);
}

/*
 * deadlock(), this method is used to check the waiting transaction for deadlock
 * go through the wtable and create a graph
 * use the traverse method to find the victim
 * if the transaction tid matches to that of the victim then
 * set the status of transaction to TR_ABORT
 * use the tp->free_locks() to release the lock owned by that transaction
 */
int wait_for::deadlock() {
    node* np;
    zgt_tx *tp;

    fprintf(stdout, "::entering deadlock method:::\n");
    fflush(stdout);
    for (np = wtable; np != NULL; np = np->next) {
        if (np->level == 0) {
            head = new node;
            head->tid = np->tid;
            head->sgno = np->sgno;
            head->obno = np->obno;
            head->lockmode = np->lockmode;
            head->semno = np->semno;
            head->next = NULL;
            head->parent = NULL;
            head->next_s = NULL;
            head->level = 1;
            printf("\nhead****%d %d %d %c %d %d\n", head->tid, head->sgno, head->obno, head->lockmode, head->semno, head->level);
            found = FALSE;
            traverse(head);
			
			
            if ((found == TRUE)&&(victim != NULL)) 
			{
                zgt_p(0);
                fprintf(stdout, "Cycle found. Deadlock has been detected\n");
                fflush(stdout);
                
				for (tp = (zgt_tx *) ZGT_Sh->lastr; tp != NULL; tp = tp->nextr) 
				{
                    if (tp->tid == victim->tid) 	// Comparing the txn id of the victim and the txn in wait table
						break;						// Victim found, Proceed to abort it
                }
/*				
*  After finding the transaction id in wait table corresponding to victim: 
*  1> We will release all the locks held by victim txn,
*  2> Identify the transactions which are waiting on semaphore and release them
*  Finally the victim transaction is aborted
*  The process is similar to that of do_commit_abort function in zgt_tx.c
*/				
 
				fprintf(stdout, "Transaction %d is the victim. Aborting this transaction\n", tp->tid);
                fflush(stdout);
                tp->status = TR_ABORT;							// Changing the status to "Abort"
               
			    tp->free_locks();								// Releasing all the locks held by this transaction

				int i, waitTxn;
				int waitSemNum = tp->semno;  
				tp->remove_tx();								// Removing transaction entry

				// Finding and unlocking transactions waiting on this semaphore
				if (waitSemNum > -1) 
				{
					waitTxn = zgt_nwait(tp->semno);				// Retrieving the transactions which are waiting on semaphore
					if (waitTxn > 0)
						for (i=waitTxn; i>0; i--)
							zgt_v(tp->semno);					// Releasing semaphore
				}

                fprintf(stdout, "Transaction %d has been successfully aborted\n", tp->tid);
                fflush(stdout);
                zgt_v(0);
                return 1;
				
                //tp->end_tx(); //sharma, oct 14 for testing

                tp->free_locks();

                // the v operation may not free the intended process
                // one needs to be more careful here

                if (victim->lockmode == 'X') {
                    //free the semaphore
                }
                // should free all locks assoc. with tid here
                continue;
            }
        }
    }
    fprintf(stdout, "::leaving deadlock method:::\n");
    fflush(stdout);
    return (0);
}

/*
 * this method is used by deadlock() find the owners
 * of the lock which are being waiting if it is visited
 * location is not null then it means a cycle is found
 * so choose a victim set the found to TRUE and return.
 * 
 * Create a node q set its appropriate pointers
 * assign the tid, sgno,lockmode, obno, head and last
 * to q.
 * 
 * Mark the wtable by going through wtable
 * and find the tid in hashtable(hlink) equal to
 * q's tid and set the level of q to 1 set the hlink
 * pointer by traveling it to proper obno and sgno
 * as in p.
 */ 

int wait_for::traverse(node* p) {
    node *q, *last = NULL;
	zgt_tx *tp;
    zgt_hlink *sp;

    sp = ZGT_Ht->find(p->sgno, p->obno);     					// Finding transactions which are holding the same object

    while (sp != NULL) 						 					// Checking all the transactions in the Hash table
	{
        
    // Case 1 : If node is already visited
		if (visited(sp->tid) == TRUE)		
		{
            q = head;
            while (q != NULL)
			{
                if ((sp->tid == q->tid)&&(q->level >= 0))		// Matching the tid of q with the entries in hash table
				{
					if (q->level == 0)							// Check if the parent and the child node are at the same level			
						q->level = -1; 							// Decrement the level of parent by 1
								
					else if (q->level > 0) 						// A cycle exists
					{							
						found = TRUE;
						victim = choose_victim(p, q);			// Victim is chosen to abort
						return (0);
					}
				}					
                
				q = q->next;
            }
            
        }

	// Case 2 : If node is visited for first time, create a new node q and initialize its pointers
		q = new node;							
		
        if (head != NULL) 
            q->next = head;		
        else 
            q->next = NULL;	 // head does not exist, so null
		
        q->next_s = NULL;
        head = q;
        last = q;
       
	// hashtable entry is assigned to q
        q->tid = sp->tid;
		q->sgno = sp->sgno;
		q->obno = sp->obno;						
        q->lockmode = sp->lockmode;

	// wtable is assigned to q and its level is thus increased by 1
        q = wtable;
        q->level = 1;
	
	// Finding the hashtable entry to find the transaction using the same object which has been requested
        q = q->next;
        sp = sp->next;
        while (sp != NULL) {
            if ((sp->obno == p->obno)&&(sp->sgno == p->sgno)) {
                break;
            }
            sp = sp->next;
        }
    }

    q = last; // assign the last node to q to traverse further

/*
 * Use the last pointer and q's level to travel through
 * transaction list zgt_tx if the tp_status is equal to
 * TR_WAIT assign the transaction data structure to node
 * q's data structure and recursively call traverse.
 * 
 * using q to find a cycle. delete the node last
 * and assign the q's next_s to last and q=last.
 */
 
	while (q != NULL)
	{
        tp = (zgt_tx *) ZGT_Sh->lastr;		     			// Traversing along the transaction list using the last pointer
        while (tp != NULL)
		{
            if (tp->tid == q->tid)			
			{
                if (q->level >= 0)				 			// We are using the level of q to find if it is visited before
					{
						if (tp->status == 'W')   			// Checking if the current transaction is in "Wait" mode
						{
							// Assigning the transaction data structure to q's data structure
							q->sgno = tp->sgno;
							q->obno = tp->obno;
							q->lockmode = tp->lockmode;
							q->semno = tp->semno;
							q->level = p->level++;  		// Increment the level of p after assigning to q
							
							printf("Transaction %d will be traversed again\n", q->tid);
							traverse(q);					// Calling the traverse function recursively
							
							if (found)
								return (0);
						}
					}
            }
            tp = tp->nextr;
        }
    
        delete last;										// Deleting the last node  
        last = q->next_s;
        q = last;			   								// Assigning last to q
    }
};

/*
 * this method is used by traverse() to keep track of the
 * visited transaction while traversing through the wait_for
 * graph using the tid of transaction in hash table(hlink pointer)
 * similar to a DFS
 */
int wait_for::visited(long t) {
    node* n;
    for (n = head; n != NULL; n = n->next) if (t == n->tid) return (TRUE);

    return (FALSE);
};

/*
 * this method is used by the traverse() method in
 * to get the node in wtable using the tid of transaction
 * in hash table(hlink pointer).
 */
node * wait_for::location(long t) {
    node* n;
    for (n = head; n != NULL; n = n->next)
        if ((t == n->tid)&&(n->level >= 0)) return (n);
    return (NULL);
}

/*
 * this method is used by traverse method to choose a
 * victim after a deadlock is encountered while traversing
 * the wait_for graph
 */
node* wait_for::choose_victim(node* p, node* q) {
    node* n;
    int total;
    zgt_tx *tp;
    //for (n=p; n != q->parent; n=n->parent) {
    //	printf("%d  ",n->tid);
    //}
    //printf("\n");
    for (n = p; n != q->parent; n = n->parent) {
        if (n->lockmode == 'X') return (n);
        else {
            for (total = 0, tp = (zgt_tx *) ZGT_Sh->lastr; tp != NULL; tp = tp->nextr) {
                if (tp->semno == n->semno) total++;
            }

            if (total == 1) return (n);
        }
        // for the moment, we need to show that there is at least one
        // candidate process that is associated with a semaphore waited by
        // exactly one process.  The code has to be modified if we can't 
        // show this.
    }
    return (NULL);
}
