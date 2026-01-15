---
title: "Java Style Guide"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Java Code Style Guide
Every major open-source project has its own style guide: a set of conventions (sometimes arbitrary) about how to write code for that project. It is much easier to understand a large codebase when all the code in it is in a consistent style.

"Style" covers a lot of ground, from "use camelCase for variable names" to "never use global variables" to "never use exceptions".

Ranger also contains checkstyle rules in [dev-support/checkstyle.xml](https://github.com/apache/ranger/blob/master/dev-support/checkstyle.xml), and a maven plugin associated with it - `maven-checkstyle-plugin` to assist with style guide compliance. There are other code style guidelines which the rules do not capture but are recommended to follow. Below is a list of rules which were followed as part of implementing [RANGER-5017](https://issues.apache.org/jira/browse/RANGER-5017).

## Source File Structure
A source file consists of, **in order**:

- Apache License
- Package statement
- Import statements
- Exactly one top-level class

**Exactly one blank line** separates each section that is present.

## Import Statements

### No wildcard imports
**Wildcard imports**, static or otherwise, **are not used**.

### No line-wrapping
Import statements are **not line-wrapped**.

### Ordering and Spacing
Imports are ordered as follows:

- All non-static imports in a single block.
- All static imports in a single block.

If there are both static and non-static imports, a single blank line separates the two blocks. There are no other blank lines between import statements.

Within each block the imported names appear in ASCII sort order.

## Class Declaration

### Exactly one top-level class declaration
Each top-level class resides in a source file of its own.

### Ordering of class contents

- Loggers if present are always at the top.
- Static members are in a single block followed by non-static members.
- Final members come before non-final members.
- The order of access modifiers is: `public protected private default`

## Formatting

### Use of Braces
Braces are used with `if, else, for, do` and `while` statements, even when the body is empty or contains only a single statement.

### Nonempty blocks: K & R style
Braces follow the `Kernighan and Ritchie` style ([Egyptian brackets](https://blog.codinghorror.com/new-programming-jargon/#3)) for nonempty blocks and block-like constructs:

- No line break before the opening brace, except as detailed below.
- Line break after the opening brace.
- No empty line after the opening brace.
- Line break before the closing brace.
- Line break after the closing brace, *only* if that brace terminates a statement or terminates the body of a method, constructor, or named class. For example, there is *no* line break after the brace if it is followed by `else` or a comma.

### Column Limit: Set to 512
### Whitespace
#### Vertical Whitespace
A single blank line may also appear anywhere it improves readability, for example between statements to organize the code into logical subsections.

*Multiple* consecutive blank lines are **NOT** permitted.

#### Horizontal Alignment: Recommended (not enforced)
```java title="Horizontal Alignment"
private int x = 5; // this is fine
private String color = blue; // this too
 
private int        x = 5;       // permitted, but future edits
private String color = "blue";  // may leave it unaligned
```

## Naming

### Package Names
Package names use only lowercase letters and digits (no underscores). Consecutive words are simply concatenated together. For example: org.apache.ranger.rangerdb, **not** org.apache.ranger.rangerDb **or** org.apache.ranger.ranger_db

### Class Names
Class names are written in [UpperCamelCase](https://google.github.io/styleguide/javaguide.html#s5.3-camel-case).

### Method Names
Method names are written in [lowerCamelCase](https://google.github.io/styleguide/javaguide.html#s5.3-camel-case).

### Constant Names
Constant names use UPPER_SNAKE_CASE : all uppercase letters, with each word separated from the next by a single underscore.

## Programming Practices
### String Concatenation

**NOT** allowed in log statements.

*Exceptions*: allowed in `Exception/System.out.println` statements. for ex:

```java
// allowed
LOG.debug("revokeAccess as user {}", user);
LOG.error("Failed to get response, Error is : {}", e.getMessage());
// not allowed
LOG.debug("revokeAccess as user " + user);
LOG.error("Failed to get response, Error is : " + e.getMessage());
// allowed
throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
// allowed
System.out.println("Unknown callback [" + cb.getClass().getName() + "]");
```
### logger.isDebugEnabled()
logger.debug statements may be preceded by isDebugEnabled() only if debug statements involve heavy operations, for ex:

```java
if (LOG.isDebugEnabled()) {
    LOG.debug("User found from principal [{}] => user:[{}], groups:[{}]", user.getName(), userName, StringUtil.toString(groups));
}
```

### Use IntelliJ suggestions - highly recommended
