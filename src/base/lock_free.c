/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */


/*
 * lock_free.c : Lock-free structures.
 */

#include "lock_free.h"
#include "error_manager.h"

/*
 * Lock-Free Circular Queue section
 */

/*
 * lock_free_circular_queue_push () - Add new entry to queue.
 *
 * return     : True if the entry was added, false otherwise.
 * queue (in) : Lock-free circular queue.
 * data (in)  : New entry data.
 */
bool
lock_free_circular_queue_push (LOCK_FREE_CIRCULAR_QUEUE * queue, void *data)
{
  INT32 prev_tail, new_tail;
  if (queue->sync_producers)
    {
      /* Use atomic compare and swap operation to make sure that two producers
       * do not write the same entry in queue.
       * First save queue tail value, compute new tail value and then try to
       * compare and swap values. If the tail was successfully changed, the
       * data can be in the previous tail entry.
       */
      do
	{
	  if (LOCK_FREE_CIRCULAR_QUEUE_IS_FULL (queue))
	    {
	      /* The queue is already full */
	      return false;
	    }
	  /* Read current tail */
	  prev_tail = queue->tail;
	  /* Compute next tail */
	  new_tail = (prev_tail + 1) % queue->capacity;
	  /* If tail value didn't change, replace it with next tail value */
	}
      while (!ATOMIC_CAS_32 (&queue->tail, prev_tail, new_tail));
      /* Copy data in the read tail value */
      memcpy (queue->data + (prev_tail * queue->data_size), data,
	      queue->data_size);
    }
  else
    {
      /* No need to synchronize producers, just write the data at the end
       * of the queue.
       */
      if (LOCK_FREE_CIRCULAR_QUEUE_IS_FULL (queue))
	{
	  /* The queue is already full */
	  return false;
	}
      memcpy (queue->data + (queue->tail * queue->data_size), data,
	      queue->data_size);
      queue->tail = (queue->tail + 1) % queue->capacity;
    }
  return true;
}

/*
 * lock_free_circular_queue_pop () - Pop one entry from queue.
 *
 * return     : First queue entry or NULL if the queue is empty.
 * queue (in) : Lock-free circular queue.
 * data (out) : Pointer where to save popped data.
 */
bool
lock_free_circular_queue_pop (LOCK_FREE_CIRCULAR_QUEUE * queue, void *data)
{
  INT32 prev_head, new_head;
  if (queue->sync_consumers)
    {
      /* Need to synchronize consumers to avoid consuming the same entry.
       * Use atomic compare and swap: read head value, compute new head value
       * and then if the head value didn't change replace it with the new
       * value. Repeat until successful and then data can be read from
       * previous head entry.
       */
      do
	{
	  if (LOCK_FREE_CIRCULAR_QUEUE_IS_EMPTY (queue))
	    {
	      /* Queue is empty, nothing to consume */
	      return false;
	    }
	  /* Read head */
	  prev_head = queue->head;
	  /* Compute new head */
	  new_head = (prev_head + 1) % queue->capacity;
	  /* Try to replace head value */
	}
      while (!ATOMIC_CAS_32 (&queue->head, prev_head, new_head));
      /* Return data from previous head value */
      memcpy (data, &queue->data + (prev_head * queue->data_size),
	      queue->data_size);
    }
  else
    {
      /* No need to synchronize consumers, return data in current head entry.
       */
      if (LOCK_FREE_CIRCULAR_QUEUE_IS_EMPTY (queue))
	{
	  return false;
	}
      memcpy (data, queue->data + (queue->head * queue->data_size),
	      queue->data_size);
      queue->head = (queue->head + 1) % queue->capacity;
    }
  return true;
}

/*
 * lock_free_circular_queue_create () - Allocate, initialize and return
 *					a new lock-free circular queue.
 *
 * return	       : Lock-free circular queue.
 * capacity (in)       : The maximum queue capacity.
 * data_size (in)      : Size of queue entry data.
 * sync_producers (in) : True if multiple producers that need synchronization.
 * sync_consumers (in) : True if multiple consumers that need synchronization.
 */
LOCK_FREE_CIRCULAR_QUEUE *
lock_free_circular_queue_create (INT32 capacity, int data_size,
				 bool sync_producers, bool sync_consumers)
{
  LOCK_FREE_CIRCULAR_QUEUE *queue =
    (LOCK_FREE_CIRCULAR_QUEUE *) malloc (sizeof (LOCK_FREE_CIRCULAR_QUEUE));
  if (queue == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (LOCK_FREE_CIRCULAR_QUEUE));
    }
  queue->data = malloc (capacity * data_size);
  if (queue->data == NULL)
    {
      free (queue);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      capacity * data_size);
      return NULL;
    }
  queue->data_size = data_size;
  queue->capacity = capacity;
  queue->sync_producers = sync_producers;
  queue->sync_consumers = sync_consumers;
  queue->head = queue->tail = 0;

  return queue;
}

/*
 * lock_free_circular_queue_destroy () - Destroy a lock-free circular queue.
 *
 * return     : Void.
 * queue (in) : Lock-free circular queue.
 */
void
lock_free_circular_queue_destroy (LOCK_FREE_CIRCULAR_QUEUE * queue)
{
  if (queue == NULL)
    {
      return;
    }
  if (queue->data != NULL)
    {
      free (queue->data);
    }
  free (queue);
}
