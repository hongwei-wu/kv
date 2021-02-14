#ifndef __CACHE_LIST_H__
#define __CACHE_LIST_H__

struct cache_list
{
    struct cache_list* prev;
    struct cache_list* next;
};

#define list_init(l) \
	(l)->prev = (l); \
	(l)->next = (l); \

#define list_empty(h) \
	((h)->prev == (h))

#define list_insert_head(h, l) \
	(l)->next = (h)->next; \
	(l)->next->prev = (l); \
	(l)->prev = (h); \
	(h)->next = (l);

#define list_insert_after list_insert_head

#define list_insert_tail(h, l) \
	(l)->prev = (h)->prev; \
	(l)->prev->next = l; \
	(l)->next = (h); \
	(h)->prev = (l);

#define list_remove(l) \
	(l)->prev->next = (l)->next; \
	(l)->next->prev = (l)->prev; \
	(l)->prev = (l); \
	(l)->next = (l);

#define offsetof_(type, member) ((uint8_t*)&((type*)0)->member)

#define list_data(l, type, link) \
	(type *) ((uint8_t *) l - offsetof_(type, link))


#define list_first(h) (h)->next
#define list_last(h)  (h)->prev

#define list_next(l) (l)->next
#define list_prev(l) (l)->prev

#define list_sentinel(l) l

#endif//__CACHE_LIST_H__
