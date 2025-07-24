/*
 *@author  chengkenli
 *@project StarRocksAPIs
 *@package app
 *@file    cache
 *@date    2025/6/10 18:09
 */

package pool

import (
	"github.com/patrickmn/go-cache"
	"time"
)

// CacheWrapper 封装缓存结构体
type CacheWrapper struct {
	cache *cache.Cache
	name  string
}

// InstantiationCache 创建新的缓存实例
func InstantiationCache(name string, defaultExpiration, cleanupInterval time.Duration) *CacheWrapper {
	return &CacheWrapper{
		cache: cache.New(defaultExpiration, cleanupInterval),
		name:  name,
	}
}

// Set 设置缓存值
func (c *CacheWrapper) Set(key string, value interface{}, expiration time.Duration) {
	c.cache.Set(key, value, expiration)
}

// Get 获取缓存值
func (c *CacheWrapper) Get(key string) (interface{}, bool) {
	return c.cache.Get(key)
}

// Delete 删除缓存值
func (c *CacheWrapper) Delete(key string) {
	c.cache.Delete(key)
}

// Flush 清空缓存
func (c *CacheWrapper) Flush() {
	c.cache.Flush()
}

// ItemCount 获取缓存项数量
func (c *CacheWrapper) ItemCount() int {
	return c.cache.ItemCount()
}

// Name 获取缓存名称
func (c *CacheWrapper) Name() string {
	return c.name
}
