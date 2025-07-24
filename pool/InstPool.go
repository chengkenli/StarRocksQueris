/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package pipe
 *@file    pool
 *@date    2025/6/11 14:43
 */

package pool

import "sync"

// GenericPool 是一个通用的对象池，支持任意类型 T
type GenericPool[T any] struct {
	pool sync.Pool
}

// InstantiationPool 创建一个新的通用对象池
// newFunc 用于在池为空时创建新对象
func InstantiationPool[T any](newFunc func() T) *GenericPool[T] {
	return &GenericPool[T]{
		pool: sync.Pool{
			New: func() any { return newFunc() },
		},
	}
}

// Get 从池中获取一个对象
func (p *GenericPool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put 将对象放回池中
func (p *GenericPool[T]) Put(x T) {
	p.pool.Put(x)
}
