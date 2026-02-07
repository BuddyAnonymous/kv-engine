package cli

import "strings"

// parseCall parses strings like:
//
//	PUT(key,value)
//	PUT(key,"value with spaces")
//	PUT(key,value,10s)
//	GET(key)
//	DELETE(key)
//
// returns: cmd, args, ok, errMsg
func ParseCall(line string) (string, []string, bool, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil, false, ""
	}

	// allow EXIT/QUIT without parentheses
	up := strings.ToUpper(line)
	if up == "EXIT" || up == "QUIT" {
		return up, nil, true, ""
	}

	// Must contain (...) form
	open := strings.IndexByte(line, '(')
	close := strings.LastIndexByte(line, ')')
	if open <= 0 || close < open {
		return "", nil, false, "expected format CMD(arg1,arg2,...)"
	}

	cmd := strings.ToUpper(strings.TrimSpace(line[:open]))
	inside := strings.TrimSpace(line[open+1 : close])

	args, err := SplitArgsCSVLike(inside)
	if err != "" {
		return "", nil, false, err
	}
	return cmd, args, true, ""
}

// splitArgsCSVLike splits by commas, but supports quoted strings with \" and \\.
// Example: key,"hello world",10s  -> ["key", "hello world", "10s"]
func SplitArgsCSVLike(s string) ([]string, string) {
	if strings.TrimSpace(s) == "" {
		return []string{}, ""
	}

	var args []string
	var cur strings.Builder
	inQuotes := false
	escape := false

	flush := func() {
		arg := strings.TrimSpace(cur.String())
		// If quoted, we've already unescaped; just trim
		args = append(args, arg)
		cur.Reset()
	}

	for i := 0; i < len(s); i++ {
		ch := s[i]

		if escape {
			// allow \" and \\ and \n \t if you want
			switch ch {
			case 'n':
				cur.WriteByte('\n')
			case 't':
				cur.WriteByte('\t')
			default:
				cur.WriteByte(ch)
			}
			escape = false
			continue
		}

		if ch == '\\' && inQuotes {
			escape = true
			continue
		}

		if ch == '"' {
			inQuotes = !inQuotes
			continue
		}

		if ch == ',' && !inQuotes {
			flush()
			continue
		}

		cur.WriteByte(ch)
	}

	if escape {
		return nil, "unfinished escape sequence in quotes"
	}
	if inQuotes {
		return nil, "unterminated quote (\")"
	}

	flush()

	// Optional: reject empty args like PUT(,x)
	for i := range args {
		if args[i] == "" {
			return nil, "empty argument not allowed"
		}
	}

	return args, ""
}
