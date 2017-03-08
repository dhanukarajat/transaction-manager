#Implementation of a Transaction Manager and Deadlock Detector#

In this project we were asked to implement one of the lower levels of DBMS – the Transaction Manager which will be called by higher layers to handle concurrency control. For this purpose we have implemented Strict Two-Phase Locking (S2PL) protocol where an Exclusive lock is assigned for Write operations and a Shared lock is assigned for Read operations. Hence the Transaction Manager handles Locking and Releasing objects as and when necessary. 

The project was basically divided into 2 steps where step 1 was to implement the basic operation of Transaction Manager, S2PL, as explained above, while the second part was more complicated – detecting deadlock by checking for cycles in the wait-for graph and aborting a victim transaction if deadlock is detected. 

In this project we have implemented Transaction Manager and deadlock detector as follows:

##**Transaction Manager:**##

The Transaction Manager has been implemented using zgt_tx.c and zgt_tm.c programs.

•	In zgt_tx.c  program we implemented Read/Write operations using readtx() and writetx() methods . Initially we get the transaction id, object number and count for a transaction. Then we lock the transaction using zgt_p(0) (entering critical section). Then we are retrieving the existing transaction. Then we check the status of the transaction.  If current transaction is in *"Processing"* state, we are setting the lock in *"Shared"* mode.  If current transaction is in *"Waiting"* mode, we are aborting the transaction and setting the status of the transaction to *"Abort"*	. Finally we release the transaction manager resources using zgt_v(0) and close the thread which is handling the current operation. 

•	The writetx() method is similar to readtx() method. If the transaction is in *“Processing“* state, in this case we set the lock in *"Exclusive"* mode. 

•	We have implemented aborttx()and committx() methods for committing and aborting a transaction. Here we retrieve the current transaction using a pointer and check it status. If the value of the pointer is not NULL, then we invoke do_commit_abort() method to do the needful. 

•	In do_commit_abort() method, we open the log file to append the current operations going on. We retrieve the status of the transaction and update it. We release all the locks held by this transaction and then ty to remove it using removetx() method. We retrieve the transactions that are waiting on semaphore using zgt_nwait() and store it in a variable called waitTxn. Then we release all the semaphores one by one using zgt_v(0). 

•	Finally we are acquiring the necessary locks for performing read and write operations using set_lock() method. In this method, we at first try to retrieve the current owner of the lock for the requested object and if it exists, we store the value in currentOwner  variable. If the owner is same, then we change the status of the transaction to TR_ACTIVE mode. If there is no owner we add it to the Hash Table. If the owner is different, we check for the possibility of deadlock formation by invoking deadlock() method and the transaction is kept in TR_WAIT mode till then. 

•	In zgt_tm.c, we have implemented the commitTX() and TXwrite() methods which were very similar to the TXread() method. We created a thread to execute the operations implemented in zgt_tx.c file.

##**Deadlock Detector:**##

Implementing a deadlock detector was the most interesting and challenging part of the project. We implemented two methods:  deadlock() and traverse() method. 

In the deadlock() method, we use the wait table. We get the victim transaction using traverse() method. Then we compare the transaction id of the victim with the transaction id in the wait table.  After finding the transaction id in wait table corresponding to victim, we release all the locks held by victim transaction. We change the status of victim transaction to TR_ABORT mode. We release the locks held by this transaction and then ty to remove it using removetx() method. We identify and retrieve the transactions that are waiting on semaphore using zgt_nwait() and store it in a variable called waitTxn. Then we release all the semaphores one by one using zgt_v(0).  In this manner the victim transaction is aborted. The process is similar to that of do_commit_abort() method in zgt_tx.c discussed above. If a deadlock is detected, the program returns a value of 1 to transaction manager, otherwise it returns 0.

The waiting transactions are recorded in the wait table. This table is traversed to find the victim. The transactions which are obtaining locks on the object are in the Hash table. We traverse throughout the Hash table list to find whether a particular transaction node has been visited before or not. A deadlock can be easily detected by observing the number of times the node has been visited. If the transaction node is visited for first time, we create a new node q and initialize the necessary pointers. It is added to the wait table and the last node is assigned to it. We retrieve the transaction id from the Hash table which has the same id as that of q. We make use of the levels associated with every transaction node. We are traversing along the transaction list using the last pointer. The level of q is used to find if it is visited before. If the current transaction is in "Wait" mode, the transaction data structure is assigned to q's data structure. The level of the node which is being traversed is incremented and the traverse() function runs recursively. If the transaction is visited more than once, it forms a cycle and thus results in a deadlock, which has to be aborted. The choosevictim() method is invoked to choose the transaction which needs to be aborted.

**Skeleton Code provided by _Prof. Sharma Chakravarthy_ (UT Arlington)**

**Team members: Rajat Dhanuka & Anvit Bhimsain Joshi** 

