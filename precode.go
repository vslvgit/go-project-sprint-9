package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	go func() {
		defer close(ch)
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
	}()

}

// Worker читает число из канала in и пишет его в канал out.

func Worker(in <-chan int64, out chan<- int64) {
	defer close(out)

	for {
		v, ok := <-in
		if !ok {
			// Канал in закрыт, завершаем работу
			return
		}

		// Отправляем значение в выходной канал
		out <- v

		// Делаем паузу на 1 миллисекунду
		time.Sleep(time.Millisecond)
	}
}

func main() {
	chIn := make(chan int64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(ctx, chIn, func(i int64) {
		inputSum += i
		inputCount++
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go func(in <-chan int64, index int) {
			defer wg.Done()
			for value := range in {
				chOut <- value
				amounts[index]++
			}
			fmt.Println("Рабочая горутина", index, "завершила работу")
		}(outs[i], i)
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		close(chOut) // Закрываем результирующий канал
		cancel()     // Останавливаем генератор
		fmt.Println("Все рабочие горутины завершили работу")
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	for value := range chOut {
		sum += value
		count++
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
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
