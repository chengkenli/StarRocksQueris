/*
 *@author  chengkenli
 *@project StarRocksQueris
 *@package fronends
 *@file    frontendTFIDF
 *@date    2024/8/21 21:29
 */

package fronends

import (
	"math"
	"strings"
	"unicode"
)

// WordMap 用于存储单词及其TF-IDF值的映射
type WordMap map[string]float64

// 计算词频
func calculateTermFrequency(text string) WordMap {
	words := strings.FieldsFunc(text, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	wordMap := make(WordMap)
	for _, word := range words {
		wordMap[word]++
	}
	return wordMap
}

// 计算TF-IDF值。在这个简化的例子中，我们仅计算TF。
func calculateTFIDF(wordMap WordMap) WordMap {
	for word, tf := range wordMap {
		// 这里简化处理，直接用词频作为TF-IDF值
		wordMap[word] = tf
	}
	return wordMap
}

// 计算余弦相似度
func cosineSimilarity(a, b WordMap) float64 {
	// 计算两个向量的点积
	dotProduct := 0.0
	for word, aVal := range a {
		if bVal, exists := b[word]; exists {
			dotProduct += aVal * bVal
		}
	}

	// 计算两个向量的欧几里得范数
	magnitudeA := 0.0
	for _, val := range a {
		magnitudeA += val * val
	}
	magnitudeA = math.Sqrt(magnitudeA)

	magnitudeB := 0.0
	for _, val := range b {
		magnitudeB += val * val
	}
	magnitudeB = math.Sqrt(magnitudeB)

	// 防止除零错误
	if magnitudeA == 0 || magnitudeB == 0 {
		return 0
	}

	// 计算余弦相似度
	return dotProduct / (magnitudeA * magnitudeB)
}

// 将余弦相似度转换为百分比
func similarityPercentage(cosineSim float64) float64 {
	return cosineSim * 100
}

func SchemaTFIDF(t1, t2 string) float64 {
	// 计算TF-IDF值
	tfidf1 := calculateTFIDF(calculateTermFrequency(t1))
	tfidf2 := calculateTFIDF(calculateTermFrequency(t2))
	// 计算余弦相似度
	cosSim := cosineSimilarity(tfidf1, tfidf2)
	// 计算相似百分比
	similarity := similarityPercentage(cosSim)
	// 返回百分比
	return similarity
}
