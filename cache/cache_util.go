package cache

import (
	"context"
	"github.com/bsm/redislock"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/text/gstr"
	"time"
)

const ServiceCachePrefix = "_SC_"
const ServiceCacheKeySetPrefix = "_SC_SET_"
const ServiceCacheLockerPrefix = "_LOCK"

func Initialized() bool {
	return storage.Initialized()
}

// GetCacheKey 生成cacheKey
// serviceName service名称，不同service名称不要相同，否则会造成 cacheKey 冲突，可以将 serviceName 当做某些缓存实现的 namespace 看待
// funcName method名称，不同funcName名称不要相同，否则会造成 cacheKey 冲突
// funcParams 所有的method参数
func GetCacheKey(serviceName string, funcName string, funcParams ...any) *string {
	var keyModelString []string
	if funcParams != nil {
		for _, one := range funcParams {
			if one == nil {
				continue
			}
			keyModelString = append(keyModelString, gjson.MustEncodeString(one))
		}
	}
	cacheKey := ServiceCachePrefix + serviceName + "_" + funcName + "(" + gstr.Join(keyModelString, "_") + ")"
	return &cacheKey
}

// RetrieveCacheTo 根据 cacheKey 获取缓存对象，并通过 json 解码到 value 中
// value 应该是原始对象的指针，必须在外部先初始化该对象
func RetrieveCacheTo(ctx context.Context, cacheKey *string, value any) error {
	if cacheKey == nil {
		return ErrEmptyCacheKey
	}
	if !storage.Initialized() {
		return ErrCacheNotInitialized
	}
	var (
		lock         *redislock.Lock
		lockObtained = false
		err          error
	)
	for !lockObtained {
		startTimestamp := gtime.Now()
		// 对每次取 cache  给最长 3秒钟处理时间，在此期间到达的同样取 cache 请求会等待当前处理结束
		lock, err = RedisLocker.Obtain(ctx, ServiceCacheLockerPrefix+*cacheKey, LockTimeout, nil)
		if err == redislock.ErrNotObtained { // 反复等待，直到上一个同 cacheKey 的取 cache 操作结束
			time.Sleep(10 * time.Millisecond)
			if gtime.Now().After(startTimestamp.Add(LockTimeout)) { // 等待时间超过 3 秒钟，返回 nil
				g.Log().Errorf(ctx, "Timeout obtaining lock for cache \"%s\"", *cacheKey)
				return ErrLockTimeout
			}
			continue
		} else if err != nil {
			g.Log().Errorf(ctx, "Error obtaining lock for cache \"%s\"", *cacheKey)
			return err
		}
		lockObtained = true
	}
	defer func(lock *redislock.Lock, ctx context.Context) {
		_ = lock.Release(ctx)
	}(lock, ctx)

	cachedValue, err := storage.Get(ctx, *cacheKey)
	if err == ErrNilResult {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	err = gjson.DecodeTo(cachedValue, value)
	if err != nil {
		g.Log().Warningf(ctx, "error decoding cache value \"%s\" for key \"%s\"", cachedValue, *cacheKey)
		return err
	}
	return nil
}

// SaveCache 添加缓存
// serviceName service名称，需要的原因是要记录当前 service 下所有已经保存的缓存 key 的集合
// cacheKey 缓存key
// value 要放入缓存的value，保存前会对其进行 json 编码
func SaveCache(ctx context.Context, serviceName string, cacheKey *string, value any) error {
	if cacheKey == nil {
		return ErrEmptyCacheKey
	}
	if !storage.Initialized() {
		return ErrCacheNotInitialized
	}
	valueBytes, err := gjson.Encode(value)
	if err != nil {
		g.Log().Warningf(ctx, "error encoding cache value for key \"%s\"", *cacheKey)
		return err
	}
	err = storage.Set(ctx, *cacheKey, valueBytes, CacheItemTtl)
	if err != nil {
		g.Log().Warningf(ctx, "error save cache for key \"%s\", value \"%s\"", *cacheKey, valueBytes)
		return err
	}
	cacheKeysetName := ServiceCacheKeySetPrefix + serviceName
	err = storage.SAdd(ctx, cacheKeysetName, *cacheKey)
	if err != nil {
		g.Log().Warningf(ctx, "error save cache key \"%s\" to keyset \"%s\"", *cacheKey, cacheKeysetName)
		return err
	}
	return nil
}

// DeleteCache 根据 cacheKey 删除单个缓存
// cacheKey 缓存key
func DeleteCache(ctx context.Context, cacheKey *string) error {
	if cacheKey == nil {
		return ErrEmptyCacheKey
	}
	if !storage.Initialized() {
		return ErrCacheNotInitialized
	}
	err := storage.Delete(ctx, []string{*cacheKey})
	if err != nil {
		g.Log().Warningf(ctx, "error delete cache key \"%s\"", cacheKey)
		return err
	}
	return nil
}

// ClearCache 清除 service 下所有已经保存的缓存
func ClearCache(ctx context.Context, serviceName string) error {
	if !storage.Initialized() {
		return ErrCacheNotInitialized
	}
	cacheKeysetName := ServiceCacheKeySetPrefix + serviceName
	keys, err := storage.SMembers(ctx, cacheKeysetName)
	if err != nil {
		g.Log().Warningf(ctx, "error load cache keyset \"%s\"", cacheKeysetName)
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	err = storage.Delete(ctx, append(keys, cacheKeysetName))
	if err != nil {
		g.Log().Warningf(ctx, "error clear cache keys \"%s\"", gstr.Join(keys, ","))
		return err
	}
	return nil
}
