#!/bin/bash
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    GRACEFUL SHUTDOWN TEST STATISTICS                         ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "📖 РАСШИФРОВКА ПОКАЗАТЕЛЕЙ:"
echo "   • Total queries    - Всего запросов выполнено"
echo "   • ✓ Success        - Успешно выполненные запросы (обработаны корректно)"
echo "   • ⚠ UNAVAILABLE    - Запросы с ответом UNAVAILABLE (graceful shutdown работает!)"
echo "   • ⏱ Timeout        - Запросы, превысившие таймаут"
echo "   • ⏱ Overloaded     - Запросы с ошибкой перегрузки"
echo "   • ✗ Errors         - Неожиданные ошибки"
echo ""
echo "🎯 ВАЖНО: Наличие UNAVAILABLE - это ХОРОШО! Это означает, что graceful shutdown"
echo "   отработал и вернул корректный статус вместо падения или зависания."
echo ""

for chunk in test-results/py3test/test_graceful_shutdown/chunk*/testing_out_stuff/*.log; do
    if [ -f "$chunk" ]; then
        test_name=$(basename "$chunk" .log | sed 's/test_graceful_shutdown.py.TestGracefulShutdown.//')
        
        # Extract full statistics block - support both old and new formats
        stats=$(grep -A 15 "TEST STATISTICS \[$test_name\]" "$chunk" 2>/dev/null)
        if [ -z "$stats" ]; then
            # Try new format: "GRACEFUL SHUTDOWN TEST RESULTS:" or "ROLLING RESTART TEST RESULTS:"
            stats=$(grep -A 15 "TEST RESULTS:" "$chunk" 2>/dev/null)
        fi
        
        if [ -n "$stats" ]; then
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "📊 $test_name"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            
            # Parse statistics - extract only numbers after the last colon
            total=$(echo "$stats" | grep "Total queries:" | tail -1 | sed 's/.*Total queries:[[:space:]]*//' | awk '{print $1}')
            success_line=$(echo "$stats" | grep "✓ SUCCESS:" | tail -1 | sed 's/.*✓ SUCCESS:[[:space:]]*//')
            if [ -z "$success_line" ]; then
            success_line=$(echo "$stats" | grep "✓ Success:" | tail -1 | sed 's/.*✓ Success:[[:space:]]*//')
            fi
            unavail_line=$(echo "$stats" | grep "⚠ UNAVAILABLE:" | tail -1 | sed 's/.*⚠ UNAVAILABLE:[[:space:]]*//')
            timeout_line=$(echo "$stats" | grep "⌛ TIMEOUT:" | tail -1 | sed 's/.*⌛ TIMEOUT:[[:space:]]*//')
            if [ -z "$timeout_line" ]; then
            timeout_line=$(echo "$stats" | grep "⏱ Timeout:" | tail -1 | sed 's/.*⏱ Timeout:[[:space:]]*//')
            fi
            overload=$(echo "$stats" | grep "⏱ OVERLOADED:" | tail -1 | sed 's/.*⏱ OVERLOADED:[[:space:]]*//' | awk '{print $1}')
            if [ -z "$overload" ]; then
            overload=$(echo "$stats" | grep "⏱ Overloaded:" | tail -1 | sed 's/.*⏱ Overloaded:[[:space:]]*\|.*Overloaded:[[:space:]]*//' | awk '{print $1}')
            fi
            errors_line=$(echo "$stats" | grep "✗ ERRORS:" | tail -1 | sed 's/.*✗ ERRORS:[[:space:]]*//')
            if [ -z "$errors_line" ]; then
            errors_line=$(echo "$stats" | grep "✗.*Errors:" | tail -1 | sed 's/.*Errors:[[:space:]]*//')
            fi
            
            # Before/During shutdown stats (for local task test)
            before=$(echo "$stats" | grep "Before shutdown:" | sed 's/.*Before shutdown:[[:space:]]*//')
            during=$(echo "$stats" | grep "During shutdown:" | sed 's/.*During shutdown:[[:space:]]*//')
            
            if [ -n "$total" ] && [ "$total" != "" ]; then
                echo "   📊 Всего запросов:               $total"
            fi
            
            if [ -n "$success_line" ] && [ "$success_line" != "" ]; then
                echo "   ✅ Успешно выполнено:            $success_line"
            fi
            
            if [ -n "$unavail_line" ] && [ "$unavail_line" != "" ]; then
                echo "   ⚠️  UNAVAILABLE (ожидаемо):       $unavail_line"
            fi
            
            if [ -n "$timeout_line" ] && [ "$timeout_line" != "" ]; then
                echo "   ⏱️  Timeout:                      $timeout_line"
            fi
            
            if [ -n "$overload" ] && [ "$overload" != "" ]; then
                echo "   💥 Overloaded:                   $overload"
            fi
            
            if [ -n "$errors_line" ] && [ "$errors_line" != "" ]; then
                echo "   ❌ Неожиданные ошибки:           $errors_line"
            fi
            
            # Special formatting for local task test
            if [ -n "$before" ] && [ "$before" != "" ]; then
                echo "   📍 До shutdown:                  $before"
            fi
            if [ -n "$during" ] && [ "$during" != "" ]; then
                echo "   📍 Во время shutdown:            $during"
            fi
            
            echo ""
        fi
    fi
done

echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  💡 ИНТЕРПРЕТАЦИЯ РЕЗУЛЬТАТОВ:                                              ║"
echo "║                                                                              ║"
echo "║  • UNAVAILABLE 0-20%   - 🟢 Отлично! Минимальное влияние shutdown           ║"
echo "║  • UNAVAILABLE 20-40%  - 🟡 Хорошо! Shutdown работает корректно             ║"
echo "║  • UNAVAILABLE 40-60%  - 🟠 Нормально для тяжелой нагрузки                  ║"
echo "║  • UNAVAILABLE >60%    - 🔴 Много отказов, возможно нужна оптимизация       ║"
echo "║                                                                              ║"
echo "║  ⚠️  Важно: UNAVAILABLE - это НЕ ошибка! Это корректный ответ системы       ║"
echo "║     о том, что нода недоступна. Клиент должен повторить запрос на другой   ║"
echo "║     ноде. Это и есть graceful shutdown - система сообщает о своем           ║"
echo "║     состоянии вместо того, чтобы просто упасть.                             ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "📁 Для просмотра полного лога конкретного теста:"
echo "   less test-results/py3test/test_graceful_shutdown/chunkN/testing_out_stuff/*.log"
echo ""
echo "🔍 Для поиска конкретных UNAVAILABLE в логах:"
echo "   grep -r 'got UNAVAILABLE' test-results/py3test/test_graceful_shutdown/"
