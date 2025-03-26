package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Generator генерирует последовательность чисел 1, 2, 3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch) // Закрываем канал по завершении работы
	var i int64 = 1
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Генератор завершил работу")
			return
		case ch <- i:
			fn(i)
			i++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64, wg *sync.WaitGroup) {
	defer wg.Done() // Уменьшаем счетчик ожидания при завершении работы горутины
	defer close(out)

	for v := range in {
		out <- v
		time.Sleep(time.Millisecond) // Делаем паузу на 1 миллисекунду
	}
}

func main() {
	chIn := make(chan int64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // Сумма сгенерированных чисел
	var inputCount int64 // Количество сгенерированных чисел

	// Генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {
		inputSum += i
		inputCount++
	})

	const NumOut = 5 // Количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// Запуск рабочих горутин
	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go Worker(chIn, outs[i], &wg)
	}

	// Создаем дополнительные горутины для обработки чисел из `outs[i]` и записи в `chOut`
	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go func(in <-chan int64, index int) {
			defer wg.Done()
			defer close(chOut)
			for value := range in {
				chOut <- value
				amounts[index]++
			}
			fmt.Println("Рабочая горутина", index, "завершила работу")
		}(outs[i], i)
	}

	// Ожидаем завершения всех горутин и закрываем канал chOut
	go func() {
		wg.Wait()
		close(chOut)
		cancel() // Останавливаем генератор
		fmt.Println("Все рабочие горутины завершили работу")
	}()

	var count int64 // Количество чисел результирующего канала
	var sum int64   // Сумма чисел результирующего канала

	for value := range chOut {
		sum += value
		count++
	}

	fmt.Println("Количество чисел:", inputCount, count)
	fmt.Println("Сумма чисел:", inputSum, sum)
	fmt.Println("Разбивка по каналам:", amounts)

	// Проверка результатов
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
