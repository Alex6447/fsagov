# AGENTS.md - Правила для AI-агентов

## Важные правила

1. **Несоответствие с документацией**: Если в коде обнаруживается несоответствие с документацией (например, в Obsidian notes), НЕ исправлять сразу. Сначала уточнить у пользователя как правильно должно быть.

## Конфигурация

- `PAGE_SIZE = 100` - количество записей на странице
- `MAXPAGES = 20` - лимит страниц за один запрос (API ограничивает 20)
- Проверка: `if count_pages > MAXPAGES * 2:` (т.е. > 40 страниц)

## API Endpoints

- **Список записей**: `POST https://pub.fsa.gov.ru/api/v1/ral/common/showcases/get`
  - Пагинация: `offset` = `page` = номер страницы (0,1,2...)
  - `limit` = количество записей (макс 100)
  - Фильтры:
    - `idAddressSubject` - UUID региона (masterId из NSI)
    - `idStatus` - ID статуса [1, 6, 14, 15, 19]
    - `isGovernmentCompany` - [true/false]

- **Регионы (NSI)**: `POST https://pub.fsa.gov.ru/nsi/api/tree/federalDistrictsAndSubjects/get`
  - `parentId` - ID округа (dic_okrug_ru_X)
  - Возвращает регионы с полями `id`, `masterId`, `name`
  - Для фильтра использовать `masterId`

- **Расширенные данные**: `GET https://pub.fsa.gov.ru/api/v1/ral/common/companies/{id}`
  - Извлекать:
    - `headPersonFIO`: surname + name + patronymic
    - `contactPhone`: contacts[idType=1][0].value
    - `contactEmail`: contacts[idType=4][0].value

## Статусы (полный список)

```python
full_status_list = [
    (1, "Архивный"),
    (6, "Действует"),
    (14, "Прекращен"),
    (15, "Приостановлен"),
    (19, "Частично приостановлен"),
]
```

## Логика парсинга

См. файл `Описание 2.md` в Obsidian, раздел "Описаине 2" начиная со строки 417.

Основная логика:
1. Фильтр по городу (region) → если страниц > MAXPAGES * 2 (40)
2. Добавить фильтр по статусу (status) → если страниц > 40
3. Добавить фильтр по гос-компании (isGovernmentCompany)
4. Парсить до 20 страниц (desc), если страниц > MAXPAGES (20) - инвертировать сортировку (asc)

## Логирование

При отладке использовать:
- `REQUEST: {payload}` - запрос
- `RESPONSE: total=X, items=Y, pages=Z` - ответ
- `=== CHECK: ... -> pages=X ===` - проверка количества страниц
- `>>> SPLIT: ...` - применение сплита
- `>>> PARSING: ...` - парсинг данных
- `>>> REVERSE: ...` - инверсия сортировки
