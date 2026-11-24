// Package dynamodbtest provides an in-memory DynamoDB client for tests.
// It is not an AWS service emulator and makes no network calls.
package dynamodbtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	attrPK     = "PK"
	attrSK     = "SK"
	attrGSI1PK = "GSI1PK"
	attrGSI1SK = "GSI1SK"
	attrGSI2PK = "GSI2PK"
	attrGSI2SK = "GSI2SK"
	attrGSI3PK = "GSI3PK"
	attrGSI3SK = "GSI3SK"
	attrGSI4PK = "GSI4PK"
	attrGSI4SK = "GSI4SK"

	defaultTimeIndexName            = "GSI1"
	defaultConnectionRoomIndexName  = "GSI2"
	defaultSessionRoomIndexName     = "GSI3"
	defaultOfflineSequenceIndexName = "GSI4"
)

type item map[string]types.AttributeValue

// Fake evaluates the condition, update, transaction, and query expressions used
// by the DynamoDB StateStore. Fail provides deterministic API fault injection.
type Fake struct {
	mu            sync.Mutex
	items         map[string]item
	failNext      map[string][]error
	maxQueryItems int
}

// New creates an empty fake DynamoDB client.
func New() *Fake {
	return &Fake{items: make(map[string]item), failNext: make(map[string][]error)}
}

// Fail queues err for the next named DynamoDB API call.
func (f *Fake) Fail(op string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext[op] = append(f.failNext[op], err)
}

// SetMaxQueryItems caps each fake query page independently of the requested
// limit, allowing tests to exercise DynamoDB pagination.
func (f *Fake) SetMaxQueryItems(limit int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maxQueryItems = limit
}

func (f *Fake) takeFailure(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	queue := f.failNext[op]
	if len(queue) == 0 {
		return nil
	}
	err := queue[0]
	f.failNext[op] = queue[1:]
	return err
}

func cloneAttribute(value types.AttributeValue) types.AttributeValue {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: value.Value}
	case *types.AttributeValueMemberN:
		return &types.AttributeValueMemberN{Value: value.Value}
	case *types.AttributeValueMemberB:
		return &types.AttributeValueMemberB{Value: cloneBytes(value.Value)}
	case *types.AttributeValueMemberBOOL:
		return &types.AttributeValueMemberBOOL{Value: value.Value}
	case *types.AttributeValueMemberNULL:
		return &types.AttributeValueMemberNULL{Value: value.Value}
	default:
		panic(fmt.Sprintf("unsupported fake attribute %T", value))
	}
}

func cloneItem(value map[string]types.AttributeValue) item {
	if value == nil {
		return nil
	}
	clone := make(item, len(value))
	for name, attribute := range value {
		clone[name] = cloneAttribute(attribute)
	}
	return clone
}

func cloneItems(items map[string]item) map[string]item {
	clone := make(map[string]item, len(items))
	for key, value := range items {
		clone[key] = cloneItem(value)
	}
	return clone
}

func fakeItemKey(key map[string]types.AttributeValue) (string, error) {
	pk, err := requiredStringAttribute(key, attrPK)
	if err != nil {
		return "", err
	}
	sk, err := requiredStringAttribute(key, attrSK)
	if err != nil {
		return "", err
	}
	return pk + "\x00" + sk, nil
}

func conditionalError() error {
	return &types.ConditionalCheckFailedException{Message: awsString("conditional request failed")}
}

func awsString(value string) *string { return &value }

func cloneBytes(value []byte) []byte { return bytes.Clone(value) }

func isConditionalFailure(err error) bool {
	var conditional *types.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func requiredStringAttribute(attributes map[string]types.AttributeValue, name string) (string, error) {
	attribute, ok := attributes[name]
	if !ok {
		return "", nil
	}
	value, ok := attribute.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("attribute %s is not a string", name)
	}
	if value.Value == "" {
		return "", fmt.Errorf("attribute %s is required", name)
	}
	return value.Value, nil
}

func evaluateCondition(item map[string]types.AttributeValue, expression *string, names map[string]string, values map[string]types.AttributeValue) (bool, error) {
	if expression == nil || *expression == "" {
		return true, nil
	}
	parser, err := newConditionParser(*expression, names, values, item)
	if err != nil {
		return false, err
	}
	return parser.parseOr()
}

func (f *Fake) GetItem(ctx context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := f.takeFailure("GetItem"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	key, err := fakeItemKey(input.Key)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return &dynamodb.GetItemOutput{Item: cloneItem(f.items[key])}, nil
}

func putItem(items map[string]item, input *dynamodb.PutItemInput) (item, error) {
	key, err := fakeItemKey(input.Item)
	if err != nil {
		return nil, err
	}
	current := items[key]
	matches, err := evaluateCondition(current, input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, conditionalError()
	}
	items[key] = cloneItem(input.Item)
	return cloneItem(current), nil
}

func (f *Fake) PutItem(ctx context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if err := f.takeFailure("PutItem"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	old, err := putItem(f.items, input)
	if err != nil {
		return nil, err
	}
	output := &dynamodb.PutItemOutput{}
	if input.ReturnValues == types.ReturnValueAllOld {
		output.Attributes = old
	}
	return output, nil
}

func deleteItem(items map[string]item, input *dynamodb.DeleteItemInput) error {
	key, err := fakeItemKey(input.Key)
	if err != nil {
		return err
	}
	matches, err := evaluateCondition(items[key], input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	if err != nil {
		return err
	}
	if !matches {
		return conditionalError()
	}
	delete(items, key)
	return nil
}

func (f *Fake) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if err := f.takeFailure("DeleteItem"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return &dynamodb.DeleteItemOutput{}, deleteItem(f.items, input)
}

func updateItem(items map[string]item, input *dynamodb.UpdateItemInput) (item, error) {
	key, err := fakeItemKey(input.Key)
	if err != nil {
		return nil, err
	}
	current := items[key]
	matches, err := evaluateCondition(current, input.ConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, conditionalError()
	}
	updated := cloneItem(current)
	if updated == nil {
		updated = make(item)
	}
	for name, attribute := range input.Key {
		updated[name] = cloneAttribute(attribute)
	}
	if err := applyUpdate(updated, *input.UpdateExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues); err != nil {
		return nil, err
	}
	items[key] = updated
	return cloneItem(updated), nil
}

func (f *Fake) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := f.takeFailure("UpdateItem"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	updated, err := updateItem(f.items, input)
	if err != nil {
		return nil, err
	}
	output := &dynamodb.UpdateItemOutput{}
	if input.ReturnValues == types.ReturnValueAllNew || input.ReturnValues == types.ReturnValueUpdatedNew {
		output.Attributes = updated
	}
	return output, nil
}

func transactionConditionError(items []types.TransactWriteItem, failed int) error {
	reasons := make([]types.CancellationReason, len(items))
	for index := range reasons {
		code := "None"
		if index == failed {
			code = "ConditionalCheckFailed"
		}
		reasons[index].Code = &code
	}
	return &types.TransactionCanceledException{Message: awsString("transaction cancelled"), CancellationReasons: reasons}
}

func (f *Fake) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if err := f.takeFailure("TransactWriteItems"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	working := cloneItems(f.items)
	for index, operation := range input.TransactItems {
		var err error
		switch {
		case operation.ConditionCheck != nil:
			check := operation.ConditionCheck
			key, keyErr := fakeItemKey(check.Key)
			if keyErr != nil {
				return nil, keyErr
			}
			matches, evalErr := evaluateCondition(working[key], check.ConditionExpression, check.ExpressionAttributeNames, check.ExpressionAttributeValues)
			if evalErr != nil {
				return nil, evalErr
			}
			if !matches {
				err = conditionalError()
			}
		case operation.Put != nil:
			put := operation.Put
			_, err = putItem(working, &dynamodb.PutItemInput{Item: put.Item, ConditionExpression: put.ConditionExpression, ExpressionAttributeNames: put.ExpressionAttributeNames, ExpressionAttributeValues: put.ExpressionAttributeValues})
		case operation.Update != nil:
			update := operation.Update
			_, err = updateItem(working, &dynamodb.UpdateItemInput{Key: update.Key, UpdateExpression: update.UpdateExpression, ConditionExpression: update.ConditionExpression, ExpressionAttributeNames: update.ExpressionAttributeNames, ExpressionAttributeValues: update.ExpressionAttributeValues})
		case operation.Delete != nil:
			deleteOperation := operation.Delete
			err = deleteItem(working, &dynamodb.DeleteItemInput{Key: deleteOperation.Key, ConditionExpression: deleteOperation.ConditionExpression, ExpressionAttributeNames: deleteOperation.ExpressionAttributeNames, ExpressionAttributeValues: deleteOperation.ExpressionAttributeValues})
		}
		if err != nil {
			if isConditionalFailure(err) {
				return nil, transactionConditionError(input.TransactItems, index)
			}
			return nil, err
		}
	}
	f.items = working
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func indexAttributes(indexName string) (string, string) {
	switch indexName {
	case defaultTimeIndexName:
		return attrGSI1PK, attrGSI1SK
	case defaultConnectionRoomIndexName:
		return attrGSI2PK, attrGSI2SK
	case defaultSessionRoomIndexName:
		return attrGSI3PK, attrGSI3SK
	case defaultOfflineSequenceIndexName:
		return attrGSI4PK, attrGSI4SK
	default:
		return attrPK, attrSK
	}
}

func compareAttributes(left, right types.AttributeValue) int {
	switch left := left.(type) {
	case *types.AttributeValueMemberS:
		return strings.Compare(left.Value, right.(*types.AttributeValueMemberS).Value)
	case *types.AttributeValueMemberN:
		leftNumber, _, _ := big.ParseFloat(left.Value, 10, 256, big.ToNearestEven)
		rightNumber, _, _ := big.ParseFloat(right.(*types.AttributeValueMemberN).Value, 10, 256, big.ToNearestEven)
		return leftNumber.Cmp(rightNumber)
	case *types.AttributeValueMemberB:
		return bytes.Compare(left.Value, right.(*types.AttributeValueMemberB).Value)
	default:
		panic("unsupported key attribute")
	}
}

func (f *Fake) Query(ctx context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if err := f.takeFailure("Query"); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	indexName := ""
	if input.IndexName != nil {
		indexName = *input.IndexName
	}
	_, skName := indexAttributes(indexName)
	matches := make([]item, 0)
	for _, candidate := range f.items {
		if _, ok := candidate[attrPK]; !ok {
			continue
		}
		if indexName != "" {
			if _, ok := candidate[skName]; !ok {
				continue
			}
		}
		matched, err := evaluateCondition(candidate, input.KeyConditionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, cloneItem(candidate))
		}
	}
	forward := input.ScanIndexForward == nil || *input.ScanIndexForward
	sort.Slice(matches, func(i, j int) bool {
		comparison := compareAttributes(matches[i][skName], matches[j][skName])
		if forward {
			return comparison < 0
		}
		return comparison > 0
	})
	if len(input.ExclusiveStartKey) != 0 {
		startSort, ok := input.ExclusiveStartKey[skName]
		if !ok {
			return nil, fmt.Errorf("exclusive start key lacks %s", skName)
		}
		position := sort.Search(len(matches), func(index int) bool {
			comparison := compareAttributes(matches[index][skName], startSort)
			if forward {
				return comparison > 0
			}
			return comparison < 0
		})
		matches = matches[position:]
	}
	limit := len(matches)
	if input.Limit != nil && int(*input.Limit) < limit {
		limit = int(*input.Limit)
	}
	if f.maxQueryItems > 0 && f.maxQueryItems < limit {
		limit = f.maxQueryItems
	}
	if limit < 0 {
		limit = 0
	}
	page := matches[:limit]
	output := &dynamodb.QueryOutput{Items: make([]map[string]types.AttributeValue, len(page))}
	for index, attributes := range page {
		output.Items[index] = cloneItem(attributes)
	}
	if limit < len(matches) && len(page) != 0 {
		last := page[len(page)-1]
		output.LastEvaluatedKey = map[string]types.AttributeValue{
			attrPK: cloneAttribute(last[attrPK]), attrSK: cloneAttribute(last[attrSK]), skName: cloneAttribute(last[skName]),
		}
	}
	return output, nil
}

func applyUpdate(attributes item, expression string, names map[string]string, values map[string]types.AttributeValue) error {
	expression = strings.TrimSpace(expression)
	if strings.HasPrefix(expression, "ADD ") {
		for _, term := range strings.Split(strings.TrimPrefix(expression, "ADD "), ",") {
			parts := strings.Fields(term)
			if len(parts) != 2 {
				return fmt.Errorf("invalid ADD term %q", term)
			}
			name := resolveName(parts[0], names)
			increment, err := numberAsBigInt(values[parts[1]])
			if err != nil {
				return err
			}
			current := big.NewInt(0)
			if attributes[name] != nil {
				current, err = numberAsBigInt(attributes[name])
				if err != nil {
					return err
				}
			}
			attributes[name] = &types.AttributeValueMemberN{Value: current.Add(current, increment).String()}
		}
		return nil
	}
	if !strings.HasPrefix(expression, "SET ") {
		return fmt.Errorf("unsupported update expression %q", expression)
	}
	for _, term := range splitExpressionTerms(strings.TrimPrefix(expression, "SET ")) {
		parts := strings.SplitN(term, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid SET term %q", term)
		}
		name := resolveName(strings.TrimSpace(parts[0]), names)
		rhs := strings.TrimSpace(parts[1])
		switch {
		case strings.HasPrefix(rhs, "if_not_exists("):
			arguments := strings.Split(strings.TrimSuffix(strings.TrimPrefix(rhs, "if_not_exists("), ")"), ",")
			if len(arguments) != 2 {
				return fmt.Errorf("invalid if_not_exists %q", rhs)
			}
			if _, exists := attributes[resolveName(strings.TrimSpace(arguments[0]), names)]; !exists {
				attributes[name] = cloneAttribute(values[strings.TrimSpace(arguments[1])])
			}
		case strings.Contains(rhs, " + "):
			addends := strings.Split(rhs, " + ")
			current, err := numberAsBigInt(attributes[resolveName(strings.TrimSpace(addends[0]), names)])
			if err != nil {
				return err
			}
			increment, err := numberAsBigInt(values[strings.TrimSpace(addends[1])])
			if err != nil {
				return err
			}
			attributes[name] = &types.AttributeValueMemberN{Value: current.Add(current, increment).String()}
		default:
			value, ok := values[rhs]
			if !ok {
				return fmt.Errorf("missing update value %q", rhs)
			}
			attributes[name] = cloneAttribute(value)
		}
	}
	return nil
}

func numberAsBigInt(value types.AttributeValue) (*big.Int, error) {
	number, ok := value.(*types.AttributeValueMemberN)
	if !ok {
		return nil, fmt.Errorf("not a number: %T", value)
	}
	parsed, ok := new(big.Int).SetString(number.Value, 10)
	if !ok {
		return nil, fmt.Errorf("invalid number %q", number.Value)
	}
	return parsed, nil
}

func splitExpressionTerms(expression string) []string {
	var terms []string
	depth := 0
	start := 0
	for index, r := range expression {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				terms = append(terms, strings.TrimSpace(expression[start:index]))
				start = index + 1
			}
		}
	}
	return append(terms, strings.TrimSpace(expression[start:]))
}

func resolveName(token string, names map[string]string) string {
	if strings.HasPrefix(token, "#") {
		return names[token]
	}
	return token
}

type conditionParser struct {
	tokens []string
	index  int
	names  map[string]string
	values map[string]types.AttributeValue
	item   map[string]types.AttributeValue
}

func newConditionParser(expression string, names map[string]string, values map[string]types.AttributeValue, item map[string]types.AttributeValue) (*conditionParser, error) {
	tokens, err := expressionTokens(expression)
	if err != nil {
		return nil, err
	}
	return &conditionParser{tokens: tokens, names: names, values: values, item: item}, nil
}

func expressionTokens(expression string) ([]string, error) {
	var tokens []string
	for index := 0; index < len(expression); {
		r := rune(expression[index])
		if unicode.IsSpace(r) {
			index++
			continue
		}
		if strings.ContainsRune("(),", r) {
			tokens = append(tokens, string(r))
			index++
			continue
		}
		if strings.ContainsRune("=<>", r) {
			start := index
			index++
			if index < len(expression) && (expression[index] == '=' || (expression[start] == '<' && expression[index] == '>')) {
				index++
			}
			tokens = append(tokens, expression[start:index])
			continue
		}
		start := index
		for index < len(expression) {
			r = rune(expression[index])
			if unicode.IsSpace(r) || strings.ContainsRune("(),=<>", r) {
				break
			}
			index++
		}
		if start == index {
			return nil, fmt.Errorf("invalid expression at %d", index)
		}
		tokens = append(tokens, expression[start:index])
	}
	return tokens, nil
}

func (p *conditionParser) peek(token string) bool {
	return p.index < len(p.tokens) && strings.EqualFold(p.tokens[p.index], token)
}

func (p *conditionParser) take() string {
	token := p.tokens[p.index]
	p.index++
	return token
}

func (p *conditionParser) parseOr() (bool, error) {
	value, err := p.parseAnd()
	if err != nil {
		return false, err
	}
	for p.peek("OR") {
		p.take()
		right, err := p.parseAnd()
		if err != nil {
			return false, err
		}
		value = value || right
	}
	if p.index != len(p.tokens) {
		return false, fmt.Errorf("unexpected expression token %q", p.tokens[p.index])
	}
	return value, nil
}

func (p *conditionParser) parseAnd() (bool, error) {
	value, err := p.parseUnary()
	if err != nil {
		return false, err
	}
	for p.peek("AND") {
		p.take()
		right, err := p.parseUnary()
		if err != nil {
			return false, err
		}
		value = value && right
	}
	return value, nil
}

func (p *conditionParser) parseUnary() (bool, error) {
	if p.peek("NOT") {
		p.take()
		value, err := p.parseUnary()
		return !value, err
	}
	return p.parsePrimary()
}

func (p *conditionParser) parsePrimary() (bool, error) {
	if p.peek("(") {
		p.take()
		value, err := p.parseAnd()
		if err != nil {
			return false, err
		}
		for !p.peek("OR") && !p.peek(")") {
			right, err := p.parseAnd()
			if err != nil {
				return false, err
			}
			value = value && right
		}
		if p.peek("OR") {
			// Parenthesized expressions in Store never require this fallback, but
			// handling it makes the fake less surprising for future expressions.
			for p.peek("OR") {
				p.take()
				right, err := p.parseAnd()
				if err != nil {
					return false, err
				}
				value = value || right
			}
		}
		if !p.peek(")") {
			return false, fmt.Errorf("missing closing parenthesis")
		}
		p.take()
		return value, nil
	}
	if p.index >= len(p.tokens) {
		return false, fmt.Errorf("unexpected end of expression")
	}
	token := p.take()
	if token == "attribute_exists" || token == "attribute_not_exists" || token == "begins_with" {
		if !p.peek("(") {
			return false, fmt.Errorf("function %s lacks parenthesis", token)
		}
		p.take()
		name := resolveName(p.take(), p.names)
		_, exists := p.item[name]
		result := exists
		if token == "attribute_not_exists" {
			result = !exists
		}
		if token == "begins_with" {
			if !p.peek(",") {
				return false, fmt.Errorf("begins_with lacks value")
			}
			p.take()
			prefix := p.values[p.take()].(*types.AttributeValueMemberS).Value
			actual, ok := p.item[name].(*types.AttributeValueMemberS)
			result = ok && strings.HasPrefix(actual.Value, prefix)
		}
		if !p.peek(")") {
			return false, fmt.Errorf("function %s lacks closing parenthesis", token)
		}
		p.take()
		return result, nil
	}
	left, leftExists := p.operand(token)
	if p.index >= len(p.tokens) {
		return false, fmt.Errorf("operand %q lacks comparison", token)
	}
	operator := p.take()
	rightToken := p.take()
	right, rightExists := p.operand(rightToken)
	if !leftExists || !rightExists {
		return false, nil
	}
	comparison := compareAttributes(left, right)
	switch operator {
	case "=":
		return comparison == 0, nil
	case "<>":
		return comparison != 0, nil
	case "<":
		return comparison < 0, nil
	case "<=":
		return comparison <= 0, nil
	case ">":
		return comparison > 0, nil
	case ">=":
		return comparison >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison %q", operator)
	}
}

func (p *conditionParser) operand(token string) (types.AttributeValue, bool) {
	if strings.HasPrefix(token, ":") {
		value, ok := p.values[token]
		return value, ok
	}
	value, ok := p.item[resolveName(token, p.names)]
	return value, ok
}

func fakeNumber(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}
