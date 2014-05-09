# Asterix: Using Functions #

## <a id="toc">Table of Contents</a> ##

* [Numeric Functions](#NumericFunctions)
* [String Functions](#StringFunctions)
* [Aggregate Functions](#AggregateFunctions)
* [Spatial Functions](#SpatialFunctions)
* [Similarity Functions](#SimilarityFunctions)
* [Tokenizing Functions](#TokenizingFunctions)
* [Temporal Functions](#TemporalFunctions)
* [Other Functions](#OtherFunctions)

Asterix provides various classes of functions to support operations on numeric, string, spatial, and temporal data. This document explains how to use these functions.

## <a id="NumericFunctions">Numeric Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### numeric-abs ###
 * Syntax:

        numeric-abs(numeric_expression)

 * Computes the absolute value of the argument.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The absolute value of the argument with the same type as the input argument, or `null` if the argument is a `null` value.

 * Example:

        let $v1 := numeric-abs(2013)
        let $v2 := numeric-abs(-4036)
        let $v3 := numeric-abs(0)
        let $v4 := numeric-abs(float("-2013.5"))
        let $v5 := numeric-abs(double("-2013.593823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": 4036, "v3": 0, "v4": 2013.5f, "v5": 2013.5938237483274d }


### numeric-ceiling ###
 * Syntax:

        numeric-ceiling(numeric_expression)

 * Computes the smallest (closest to negative infinity) number with no fractional part that is not less than the value of the argument. If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The ceiling value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := numeric-ceiling(2013)
        let $v2 := numeric-ceiling(-4036)
        let $v3 := numeric-ceiling(0.3)
        let $v4 := numeric-ceiling(float("-2013.2"))
        let $v5 := numeric-ceiling(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2013.0d }


### numeric-floor ###
 * Syntax:

        numeric-floor(numeric_expression)

 * Computes the largest (closest to positive infinity) number with no fractional part that is not greater than the value. If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The floor value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := numeric-floor(2013)
        let $v2 := numeric-floor(-4036)
        let $v3 := numeric-floor(0.8)
        let $v4 := numeric-floor(float("-2013.2"))
        let $v5 := numeric-floor(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 0.0d, "v4": -2014.0f, "v5": -2014.0d }


### numeric-round ###
 * Syntax:

        numeric-round(numeric_expression)

 * Computes the number with no fractional part that is closest (and also closest to positive infinity) to the argument.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
 * Return Value:
    * The rounded value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := numeric-round(2013)
        let $v2 := numeric-round(-4036)
        let $v3 := numeric-round(0.8)
        let $v4 := numeric-round(float("-2013.256"))
        let $v5 := numeric-round(double("-2013.893823748327284"))
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2014.0d }


### numeric-round-half-to-even ###
 * Syntax:

        numeric-round-half-to-even(numeric_expression, [precision])

 * Computes the closest numeric value to `numeric_expression` that is a multiple of ten to the power of minus `precision`. `precision` is optional and by default value `0` is used.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64`/`float`/`double` value.
    * `precision`: An optional integer field representing the number of digits in the fraction of the the result
 * Return Value:
    * The rounded value for the given number in the same type as the input argument, or `null` if the input is `null`.

 * Example:

        let $v1 := numeric-round-half-to-even(2013)
        let $v2 := numeric-round-half-to-even(-4036)
        let $v3 := numeric-round-half-to-even(0.8)
        let $v4 := numeric-round-half-to-even(float("-2013.256"))
        let $v5 := numeric-round-half-to-even(double("-2013.893823748327284"))
        let $v6 := numeric-round-half-to-even(double("-2013.893823748327284"), 2)
        let $v7 := numeric-round-half-to-even(2013, 4)
        let $v8 := numeric-round-half-to-even(float("-2013.256"), 5)
        return { "v1": $v1, "v2": $v2, "v3": $v3, "v4": $v4, "v5": $v5, "v6": $v6, "v7": $v7, "v8": $v8 }


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0d, "v4": -2013.0f, "v5": -2014.0d, "v6": -2013.89d, "v7": 2013, "v8": -2013.256f }


## <a id="StringFunctions">String Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### string-to-codepoint ###
 * Syntax:

        string-to-codepoint(string_expression)

 * Converts the string `string_expression` to its code-based representation.
 * Arguments:
    * `string_expression` : A `string` that will be converted.
 * Return Value:
    * An `OrderedList` of the code points for the string `string_expression`.

### codepoint-to-string ###
 * Syntax:

        codepoint-to-string(list_expression)

 * Converts the ordered code-based representation `list_expression` to the corresponding string.
 * Arguments:
    * `list_expression` : An `OrderedList` of code-points.
 * Return Value:
    * A `string` representation of `list_expression`.

 * Example:

        use dataverse TinySocial;

        let $s := "Hello ASTERIX!"
        let $l := string-to-codepoint($s)
        let $ss := codepoint-to-string($l)
        return {"codes": $l, "string": $ss}


 * The expected result is:

        { "codes": [ 72, 101, 108, 108, 111, 32, 65, 83, 84, 69, 82, 73, 88, 33 ], "string": "Hello ASTERIX!" }


### contains ###
 * Syntax:

        contains(string_expression, substring_to_contain)

 * Checks whether the string `string_expression` contains the string `substring_to_contain`
 * Arguments:
    * `string_expression` : A `string` that might contain the given substring.
    * `substring_to_contain` : A target `string` that might be contained.
 * Return Value:
    * A `boolean` value, `true` if `string_expression` contains `substring_to_contain`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "phone")
        return {"mid": $i.message-id, "message": $i.message}


 * The expected result is:

        { "mid": 2, "message": " dislike iphone its touch-screen is horrible" }
        { "mid": 13, "message": " dislike iphone the voice-command is bad:(" }
        { "mid": 15, "message": " like iphone the voicemail-service is awesome" }


### like ###
 * Syntax:

        like(string_expression, string_pattern)

 * Checks whether the string `string_expression` contains the string pattern `string_pattern`. Compared to the `contains` function, the `like` function also supports regular expressions.
 * Arguments:
    * `string_expression` : A `string` that might contain the pattern or `null`.
    * `string_pattern` : A pattern `string` that might be contained or `null`.
 * Return Value:
    * A `boolean` value, `true` if `string_expression` contains the pattern `string_pattern`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where like($i.message, "%at&t%")
        return $i.message


 * The expected result is:

        " can't stand at&t the network is horrible:("
        " can't stand at&t its plan is terrible"
        " love at&t its 3G is good:)"


### starts-with ###
 * Syntax:

        starts-with(string_expression, substring_to_start_with)

 * Checks whether the string `string_expression` starts with the string `substring_to_start_with`.
 * Arguments:
    * `string_expression` : A `string` that might start with the given string.
    * `substring_to_start_with` : A `string` that might be contained as the starting substring.
 * Return Value:
    * A `boolean`, returns `true` if `string_expression` starts with the string `substring_to_start_with`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where starts-with($i.message, " like")
        return $i.message


 * The expected result is:

        " like samsung the plan is amazing"
        " like t-mobile its platform is mind-blowing"
        " like verizon the 3G is awesome:)"
        " like iphone the voicemail-service is awesome"


### ends-with ###
 * Syntax:

        ends-with(string_expression, substring_to_end_with)

 * Checks whether the string `string_expression` ends with the string `substring_to_end_with`.
 * Arguments:
    * `string_expression` : A `string` that might end with the given string.
    * `substring_to_end_with` : A `string` that might be contained as the ending substring.
 * Return Value:
    * A `boolean`, returns `true` if `string_expression` ends with the string `substring_to_end_with`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where ends-with($i.message, ":)")
        return $i.message


 * The expected result is:

        " love sprint its shortcut-menu is awesome:)"
        " like verizon the 3G is awesome:)"
        " love at&t its 3G is good:)"


### string-concat ###
 * Syntax:

        string-concat(list_expression)

 * Concatenates a list of strings `list_expression` into a single string.
 * Arguments:
    * `list_expression` : An `OrderedList` or `UnorderedList` of `string`s (could be `null`) to be concatenated.
 * Return Value:
    * Returns the concatenated `string` value.

 * Example:

        let $i := "ASTERIX"
        let $j := " "
        let $k := "ROCKS!"
        return string-concat([$i, $j, $k])


 * The expected result is:

        "ASTERIX ROCKS!"


### string-join ###
 * Syntax:

        string-join(list_expression, string_expression)

 * Joins a list of strings `list_expression` with the given separator `string_expression` into a single string.
 * Arguments:
    * `list_expression` : An `OrderedList` or `UnorderedList` of strings (could be `null`) to be joined.
    * `string_expression` : A `string` as the separator.
 * Return Value:
    * Returns the joined `String`.

 * Example:

        use dataverse TinySocial;

        let $i := ["ASTERIX", "ROCKS~"]
        return string-join($i, "!! ")


 * The expected result is:

        "ASTERIX!! ROCKS~"


### lowercase ###
 * Syntax:

        lowercase(string_expression)

 * Converts a given string `string_expression` to its lowercase form.
 * Arguments:
    * `string_expression` : A `string` to be converted.
 * Return Value:
    * Returns a `string` as the lowercase form of the given `string_expression`.

 * Example:

        use dataverse TinySocial;

        let $i := "ASTERIX"
        return lowercase($i)


 * The expected result is:

        asterix


### matches ###
 * Syntax:

        matches(string_expression, string_pattern)

 * Checks whether the strings `string_expression` matches the given pattern `string_pattern`.
 * Arguments:
    * `string_expression` : A `string` that might contain the pattern.
    * `string_pattern` : A pattern `string` to be matched.
 * Return Value:
    * A `boolean`, returns `true` if `string_expression` matches the pattern `string_pattern`, and `false` otherwise.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where matches($i.message, "dislike iphone")
        return $i.message


 * The expected result is:

        " dislike iphone its touch-screen is horrible"
        " dislike iphone the voice-command is bad:("


### replace ###
 * Syntax:

        replace(string_expression, string_pattern, string_replacement)

 * Checks whether the string `string_expression` matches the given pattern `string_pattern`, and replace the matched pattern `string_pattern` with the new pattern `string_replacement`.
 * Arguments:
    * `string_expression` : A `string` that might contain the pattern.
    * `string_pattern` : A pattern `string` to be matched.
    * `string_replacement` : A pattern `string` to be used as the replacement.
 * Return Value:
    * Returns a `string` that is obtained after the replacements.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where matches($i.message, " like iphone")
        return replace($i.message, " like iphone", "like android")


 * The expected result is:

        "like android the voicemail-service is awesome"


### string-length ###
 * Syntax:

        string-length(string_expression)

 * Returns the length of the string `string_expression`.
 * Arguments:
    * `string_expression` : A `string` or `null` that represents the string to be checked.
 * Return Value:
    * An `int32` that represents the length of `string_expression`.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        return {"mid": $i.message-id, "message-len": string-length($i.message)}


 * The expected result is:

        { "mid": 1, "message-len": 43 }
        { "mid": 2, "message-len": 44 }
        { "mid": 3, "message-len": 33 }
        { "mid": 4, "message-len": 43 }
        { "mid": 5, "message-len": 46 }
        { "mid": 6, "message-len": 43 }
        { "mid": 7, "message-len": 37 }
        { "mid": 8, "message-len": 33 }
        { "mid": 9, "message-len": 34 }
        { "mid": 10, "message-len": 50 }
        { "mid": 11, "message-len": 38 }
        { "mid": 12, "message-len": 52 }
        { "mid": 13, "message-len": 42 }
        { "mid": 14, "message-len": 27 }
        { "mid": 15, "message-len": 45 }


### substring ###
 * Syntax:

        substring(string_expression, offset[, length])

 * Returns the substring from the given string `string_expression` based on the given start offset `offset` with the optional `length`.
 * Arguments:
    * `string_expression` : A `string` to be extracted.
    * `offset` : An `int32` as the starting offset of the substring in `string_expression`.
    * `length` : (Optional) An `int32` as the length of the substring.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where string-length($i.message) > 50
        return substring($i.message, 50)


 * The expected result is:

        "G:("


### substring-before ###
 * Syntax:

        substring-before(string_expression, string_pattern)

 * Returns the substring from the given string `string_expression` before the given pattern `string_pattern`.
 * Arguments:
    * `string_expression` : A `string` to be extracted.
    * `string_pattern` : A `string` pattern to be searched.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-before($i.message, "iphone")


 * The expected result is:

        " dislike "
        " dislike "
        " like "


### substring-after ###
 * Syntax:

        substring-after(string_expression, string_pattern)

 * Returns the substring from the given string `string_expression` after the given pattern `string_pattern`.
 * Arguments:
    * `string_expression` : A `string` to be extracted.
    * `string_pattern` : A `string` pattern to be searched.
 * Return Value:
    * A `string` that represents the substring.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookMessages')
        where contains($i.message, "iphone")
        return substring-after($i.message, "iphone")


 * The expected result is:

        " its touch-screen is horrible"
        " the voice-command is bad:("
        " the voicemail-service is awesome"

## <a id="AggregateFunctions">Aggregate Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### count ###
 * Syntax:

        count(list)

 * Gets the number of items in the given list.
 * Arguments:
    * `list`: An `orderedList` or `unorderedList` containing the items to be counted, or a `null` value.
 * Return Value:
    * An `int64` value representing the number of items in the given list. `0i64` is returned if the input is `null`.

 * Example:

        use dataverse TinySocial;

        let $l1 := ['hello', 'world', 1, 2, 3]
        let $l2 := for $i in dataset TwitterUsers return $i
        return {"count1": count($l1), "count2": count($l2)}

 * The expected result is:

        { "count1": 5i64, "count2": 4i64 }

### avg ###
 * Syntax:

        avg(num_list)

 * Gets the average value of the items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * An `double` value representing the average of the numbers in the given list. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i.friends_count
        return {"avg_friend_count": avg($l)}

 * The expected result is:

        { "avg_friend_count": 191.5d }

### sum ###
 * Syntax:

        sum(num_list)

 * Gets the sum of the items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * The sum of the numbers in the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`float`->`double`, `int32`->`int64`->`double`) among items. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i.friends_count
        return {"sum_friend_count": sum($l)}

 * The expected result is:

        { "sum_friend_count": 766 }

### min/max ###
 * Syntax:

        min(num_list), max(num_list)

 * Gets the min/max value of numeric items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing the items to be compared, or a `null` value.
 * Return Value:
    * The min/max value of the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`float`->`double`, `int32`->`int64`->`double`) among items. `null` is returned if the input is `null`, or the input list contains `null`. Non-numeric types in the input list will cause an error.

 * Example:

        use dataverse TinySocial;

        let $l := for $i in dataset TwitterUsers return $i. friends_count
        return {"min_friend_count": min($l), "max_friend_count": max($l)}

 * The expected result is:

        { "min_friend_count": 18, "max_friend_count": 445 }


### sql-count ###
 * Syntax:

        sql-count(list)

 * Gets the number of non-null items in the given list.
 * Arguments:
    * `list`: An `orderedList` or `unorderedList` containing the items to be counted, or a `null` value.
 * Return Value:
    * An `int64` value representing the number of non-null items in the given list. The value `0i64` is returned if the input is `null`.

 * Example:


        let $l1 := ['hello', 'world', 1, 2, 3, null]
        return {"count": sql-count($l1)}

 * The expected result is:

        { "count": 5i64 }


### sql-avg ###

 * Syntax:

        sql-avg(num_list)

 * Gets the average value of the non-null items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * A `double` value representing the average of the non-null numbers in the given list. The `null` value is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"avg": sql-avg($l)}

 * The expected result is:

        { "avg": 1.725d }


### sql-sum ###
 * Syntax:

        sql-sum(num_list)

 * Gets the sum of the non-null items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing numeric or null values, or a `null` value.
 * Return Value:
    * The sum of the non-null numbers in the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`float`->`double`, `int32`->`int64`->`double`) among items. The value `null` is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"sum": sql-sum($l)}

 * The expected result is:

        { "sum": 6.9d }


### sql-min/max ###
 * Syntax:

        sql-min(num_list), sql-max(num_list)

 * Gets the min/max value of the non-null numeric items in the given list.
 * Arguments:
    * `num_list`: An `orderedList` or `unorderedList` containing the items to be compared, or a `null` value.
 * Return Value:
    * The min/max value of the given list. The returning type is decided by the item type with the highest order in the numeric type promotion order (`int8`-> `int16`->`int32`->`float`->`double`, `int32`->`int64`->`double`) among items. The value `null` is returned if the input is `null`. Non-numeric types in the input list will cause an error.

 * Example:

        let $l := [1.2, 2.3, 3.4, 0, null]
        return {"min": sql-min($l), "max": sql-max($l)}

 * The expected result is:

        { "min": 0.0d, "max": 3.4d }

## <a id="SpatialFunctions">Spatial Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### create-point ###
 * Syntax:

        create-point(x, y)

 * Creates the primitive type `point` using an `x` and `y` value.
 * Arguments:
   * `x` : A `double` that represents the x-coordinate.
   * `y` : A `double` that represents the y-coordinate.
 * Return Value:
   * A `point` representing the ordered pair (`x`, `y`).

 * Example:

        use dataverse TinySocial;

        let $c :=  create-point(30.0,70.0)
        return {"point": $c}


 * The expected result is:

        { "point": point("30.0,70.0") }


### create-line ###
 * Syntax:

        create-line(point_expression1, point_expression2)

 * Creates the primitive type `line` using `point_expression1` and `point_expression2`.
 * Arguments:
    * `point_expression1` : A `point` that represents the start point of the line.
    * `point_expression2` : A `point` that represents the end point of the line.
 * Return Value:
    * A spatial `line` created using the points provided in `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-line(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"line": $c}


 * The expected result is:

        { "line": line("30.0,70.0 50.0,90.0") }


### create-rectangle ###
 * Syntax:

        create-rectangle(point_expression1, point_expression2)

 * Creates the primitive type `rectangle` using `point_expression1` and `point_expression2`.
 * Arguments:
    * `point_expression1` : A `point` that represents the lower-left point of the rectangle.
    * `point_expression2` : A `point` that represents the upper-right point of the rectangle.
 * Return Value:
    * A spatial `rectangle` created using the points provided in `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-rectangle(create-point(30.0,70.0), create-point(50.0,90.0))
        return {"rectangle": $c}


 * The expected result is:

        { "rectangle": rectangle("30.0,70.0 50.0,90.0") }


### create-circle ###
 * Syntax:

        create-circle(point_expression, radius)

 * Creates the primitive type `circle` using `point_expression` and `radius`.
 * Arguments:
    * `point_expression` : A `point` that represents the center of the circle.
    * `radius` : A `double` that represents the radius of the circle.
 * Return Value:
    * A spatial `circle` created using the center point and the radius provided in `point_expression` and `radius`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-circle(create-point(30.0,70.0), 5.0)
        return {"circle": $c}


 * The expected result is:

        { "circle": circle("30.0,70.0 5.0") }


### create-polygon ###
 * Syntax:

        create-polygon(list_expression)

 * Creates the primitive type `polygon` using the double values provided in the argument `list_expression`. Each two consecutive double values represent a point starting from the first double value in the list. Note that at least six double values should be specified, meaning a total of three points.
 * Arguments:
   * `list_expression` : An OrderedList of doubles representing the points of the polygon.
 * Return Value:
   * A `polygon`, represents a spatial simple polygon created using the points provided in `list_expression`.

 * Example:

        use dataverse TinySocial;

        let $c :=  create-polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0])
        return {"polygon": $c}


 * The expected result is:

        { "polygon": polygon("1.0,1.0 2.0,2.0 3.0,3.0 4.0,4.0") }


### point ###
 * Syntax:

        point(string_expression)

 * Constructor function for the `point` type by parsing a point string `string_expression`
 * Arguments:
    * `string_expression` : The `string` value representing a point value.
 * Return Value:
    * A `point` value represented by the given string.

 * Example:

        use dataverse TinySocial;

        let $c := point("55.05,-138.04")
        return {"point": $c}


 * The expected result is:

        { "point": point("55.05,-138.04") }


### line ###
 * Syntax:

        line(string_expression)

 * Constructor function for `line` type by parsing a line string `string_expression`
 * Arguments:
    * `string_expression` : The `string` value representing a line value.
 * Return Value:
    * A `line` value represented by the given string.

 * Example:

        use dataverse TinySocial;

        let $c := line("55.05,-138.04 13.54,-138.04")
        return {"line": $c}


 * The expected result is:

        { "line": line("55.05,-138.04 13.54,-138.04") }


### rectangle ###
 * Syntax:

        rectangle(string_expression)

 * Constructor function for `rectangle` type by parsing a rectangle string `string_expression`
 * Arguments:
    * `string_expression` : The `string` value representing a rectangle value.
 * Return Value:
    * A `rectangle` value represented by the given string.

 * Example:

        use dataverse TinySocial;

        let $c := rectangle("20.05,-125.0 40.67,-100.87")
        return {"rectangle": $c}


 * The expected result is:

        { "rectangle": rectangle("20.05,-125.0 40.67,-100.87") }


### circle ###
 * Syntax:

        circle(string_expression)

 * Constructor function for `circle` type by parsing a circle string `string_expression`
 * Arguments:
    * `string_expression` : The `string` value representing a circle value.
 * Return Value:
   * A `circle` value represented by the given string.

 * Example:

        use dataverse TinySocial;

        let $c := circle("55.05,-138.04 10.0")
        return {"circle": $c}


 * The expected result is:

        { "circle": circle("55.05,-138.04 10.0") }


### polygon ###
 * Syntax:

        polygon(string_expression)

 * Constructor function for `polygon` type by parsing a polygon string `string_expression`
 * Arguments:
    * `string_expression` : The `string` value representing a polygon value.
 * Return Value:
    * A `polygon` value represented by the given string.

 * Example:

        use dataverse TinySocial;

        let $c := polygon("55.05,-138.04 13.54,-138.04 13.54,-53.31 55.05,-53.31")
        return {"polygon": $c}


 * The expected result is:

        { "polygon": polygon("55.05,-138.04 13.54,-138.04 13.54,-53.31 55.05,-53.31") }


### get-x/get-y ###
 * Syntax:

        get-x(point_expression) or get-y(point_expression)

 * Returns the x or y coordinates of a point `point_expression`.
 * Arguments:
    * `point_expression` : A `point`.
 * Return Value:
    * A `double` representing the x or y coordinates of the point `point_expression`.

 * Example:

        use dataverse TinySocial;

        let $point := create-point(2.3,5.0)
        return {"x-coordinate": get-x($point), "y-coordinate": get-y($point)}


 * The expected result is:

        { "x-coordinate": 2.3d, "y-coordinate": 5.0d }


### get-points ###
 * Syntax:

        get-points(spatial_expression)

 * Returns an ordered list of the points forming the spatial object `spatial_expression`.
 * Arguments:
    * `spatial_expression` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * An `OrderedList` of the points forming the spatial object `spatial_expression`.

 * Example:

        use dataverse TinySocial;

        let $line := create-line(create-point(100.6,99.4), create-point(-72.0,-76.9))
        let $rectangle := create-rectangle(create-point(9.2,49.0), create-point(77.8,111.1))
        let $polygon := create-polygon([1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0])
        let $line_list := get-points($line)
        let $rectangle_list := get-points($rectangle)
        let $polygon_list := get-points($polygon)
        return {"line-first-point": $line_list[0], "line-second-point": $line_list[1], "rectangle-left-bottom-point": $rectangle_list[0], "rectangle-top-upper-point": $rectangle_list[1], "polygon-first-point": $polygon_list[0], "polygon-second-point": $polygon_list[1], "polygon-third-point": $polygon_list[2], "polygon-forth-point": $polygon_list[3]}


 * The expected result is:

        { "line-first-point": point("100.6,99.4"), "line-second-point": point("-72.0,-76.9"), "rectangle-left-bottom-point": point("9.2,49.0"), "rectangle-top-upper-point": point("77.8,111.1"), "polygon-first-point": point("1.0,1.0"), "polygon-second-point": point("2.0,2.0"), "polygon-third-point": point("3.0,3.0"), "polygon-forth-point": point("4.0,4.0") }


### get-center/get-radius ###
 * Syntax:

        get-center(circle_expression) or get-radius(circle_expression)

 * Returns the center and the radius of a circle `circle_expression`, respectively.
 * Arguments:
    * `circle_expression` : A `circle`.
 * Return Value:
    * A `point` or `double`, represent the center or radius of the circle `circle_expression`.

 * Example:

        use dataverse TinySocial;

        let $circle := create-circle(create-point(6.0,3.0), 1.0)
        return {"circle-radius": get-radius($circle), "circle-center": get-center($circle)}



 * The expected result is:

        { "circle-radius": 1.0d, "circle-center": point("6.0,3.0") }



### spatial-distance ###
 * Syntax:

        spatial-distance(point_expression1, point_expression2)

 * Returns the Euclidean distance between `point_expression1` and `point_expression2`.
 * Arguments:
    * `point_expression1` : A `point`.
    * `point_expression2` : A `point`.
 * Return Value:
    * A `double` as the Euclidean distance between `point_expression1` and `point_expression2`.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $d :=  spatial-distance($t.sender-location, create-point(30.0,70.0))
        return {"point": $t.sender-location, "distance": $d}



 * The expected result is:

        { "point": point("47.44,80.65"), "distance": 20.434678857275934d }
        { "point": point("29.15,76.53"), "distance": 6.585089217315132d }
        { "point": point("37.59,68.42"), "distance": 7.752709203884797d }
        { "point": point("24.82,94.63"), "distance": 25.168816023007512d }
        { "point": point("32.84,67.14"), "distance": 4.030533463451212d }
        { "point": point("29.72,75.8"), "distance": 5.806754687430835d }
        { "point": point("39.28,70.48"), "distance": 9.292405501268227d }
        { "point": point("40.09,92.69"), "distance": 24.832321679617472d }
        { "point": point("47.51,83.99"), "distance": 22.41250097601782d }
        { "point": point("36.21,72.6"), "distance": 6.73231758015024d }
        { "point": point("46.05,93.34"), "distance": 28.325926286707734d }
        { "point": point("36.86,74.62"), "distance": 8.270671073135482d }


### spatial-area ###
 * Syntax:

        spatial-area(spatial_2d_expression)

 * Returns the spatial area of `spatial_2d_expression`.
 * Arguments:
    * `spatial_2d_expression` : A `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * A `double` representing the area of `spatial_2d_expression`.

 * Example:

        use dataverse TinySocial;

        let $circleArea := spatial-area(create-circle(create-point(0.0,0.0), 5.0))
        return {"Area":$circleArea}



 * The expected result is:

        { "Area": 78.53981625d }


### spatial-intersect ###
 * Syntax:

        spatial-intersect(spatial_expression1, spatial_expression2)

 * Checks whether `@arg1` and `@arg2` spatially intersect each other.
 * Arguments:
    * `spatial_expression1` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
    * `spatial_expression2` : A `point`, `line`, `rectangle`, `circle`, or `polygon`.
 * Return Value:
    * A `boolean` representing whether `spatial_expression1` and `spatial_expression2` spatially overlap with each other.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        where spatial-intersect($t.sender-location, create-rectangle(create-point(30.0,70.0), create-point(40.0,80.0)))
        return $t


 * The expected result is:

        { "tweetid": "4", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("39.28,70.48"), "send-time": datetime("2011-12-26T10:10:00.000Z"), "referred-topics": {{ "sprint", "voice-command" }}, "message-text": " like sprint the voice-command is mind-blowing:)" }
        { "tweetid": "7", "user": { "screen-name": "ChangEwing_573", "lang": "en", "friends_count": 182, "statuses_count": 394, "name": "Chang Ewing", "followers_count": 32136 }, "sender-location": point("36.21,72.6"), "send-time": datetime("2011-08-25T10:10:00.000Z"), "referred-topics": {{ "samsung", "platform" }}, "message-text": " like samsung the platform is good" }
        { "tweetid": "9", "user": { "screen-name": "NathanGiesen@211", "lang": "en", "friends_count": 39339, "statuses_count": 473, "name": "Nathan Giesen", "followers_count": 49416 }, "sender-location": point("36.86,74.62"), "send-time": datetime("2012-07-21T10:10:00.000Z"), "referred-topics": {{ "verizon", "voicemail-service" }}, "message-text": " love verizon its voicemail-service is awesome" }


### spatial-cell ###
 * Syntax:

        spatial-cell(point_expression1, point_expression2, x_increment, y_increment)

 * Returns the grid cell that `point_expression1` belongs to.
 * Arguments:
    * `point_expression1` : A `point` representing the point of interest that its grid cell will be returned.
    * `point_expression2` : A `point` representing the origin of the grid.
    * `x_increment` : A `double`, represents X increments.
    * `y_increment` : A `double`, represents Y increments.
 * Return Value:
    * A `rectangle` representing the grid cell that `point_expression1` belongs to.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        group by $c :=  spatial-cell($t.sender-location, create-point(20.0,50.0), 5.5, 6.0) with $t
        let $num :=  count($t)
        return { "cell": $c, "count": $num}


 * The expected result is:

        { "cell": rectangle("20.0,92.0 25.5,98.0"), "count": 1i64 }
        { "cell": rectangle("25.5,74.0 31.0,80.0"), "count": 2i64 }
        { "cell": rectangle("31.0,62.0 36.5,68.0"), "count": 1i64 }
        { "cell": rectangle("31.0,68.0 36.5,74.0"), "count": 1i64 }
        { "cell": rectangle("36.5,68.0 42.0,74.0"), "count": 2i64 }
        { "cell": rectangle("36.5,74.0 42.0,80.0"), "count": 1i64 }
        { "cell": rectangle("36.5,92.0 42.0,98.0"), "count": 1i64 }
        { "cell": rectangle("42.0,80.0 47.5,86.0"), "count": 1i64 }
        { "cell": rectangle("42.0,92.0 47.5,98.0"), "count": 1i64 }
        { "cell": rectangle("47.5,80.0 53.0,86.0"), "count": 1i64 }




## <a id="SimilarityFunctions">Similarity Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports queries with different similarity functions, including edit distance and Jaccard.

### edit-distance ###
 * Syntax:

        edit-distance(expression1, expression2)

 * Returns the [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) of `expression1` and `expression2`.
 * Arguments:
    * `expression1` : A `string` or a homogeneous `OrderedList` of a comparable item type.
    * `expression2` : The same type as `expression1`.
 * Return Value:
    * An `int32` that represents the edit distance between `expression1` and `expression2`.

 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $ed := edit-distance($user.name, "Suzanna Tilson")
        where $ed <= 2
        return $user


 * The expected result is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


### edit-distance-check ###
 * Syntax:

        edit-distance-check(expression1, expression2, threshold)

 * Checks whether `expression1` and `expression2` have an [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) within a given threshold.  The “check” version of edit distance is faster than the "non-check" version because the former can detect whether two items satisfy a given threshold using early-termination techniques, as opposed to computing their real distance. Although possible, it is not necessary for the user to write queries using the “check” versions explicitly, since a rewrite rule can perform an appropriate transformation from a “non-check” version to a “check” version.

 * Arguments:
    * `expression1` : A `string` or a homogeneous `OrderedList` of a comparable item type.
    * `expression2` : The same type as `expression1`.
    * `threshold` : An `int32` that represents the distance threshold.
 * Return Value:
    * An `OrderedList` with two items:
        * The first item contains a `boolean` value representing whether `expression1` and `expression2` are similar.
        * The second item contains an `int32` that represents the edit distance of `expression1` and `expression2` if it is within the threshold, or 0 otherwise.

 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $ed := edit-distance-check($user.name, "Suzanna Tilson", 2)
        where $ed[0]
        return $ed[1]


 * The expected result is:

        2


### similarity-jaccard ###
 * Syntax:

        similarity-jaccard(list_expression1, list_expression2)

 * Returns the [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) of `list_expression1` and `list_expression2`.
 * Arguments:
    * `list_expression1` : An `UnorderedList` or `OrderedList`.
    * `list_expression2` : An `UnorderedList` or `OrderedList`.
 * Return Value:
    * A `float` that represents the Jaccard similarity of `list_expression1` and `list_expression2`.

 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard($user.friend-ids, [1,5,9])
        where $sim >= 0.6f
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


### similarity-jaccard-check ###
 * Syntax:

        similarity-jaccard-check(list_expression1, list_expression2, threshold)

 * Checks whether `list_expression1` and `list_expression2` have a [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index) greater than or equal to threshold.  Again, the “check” version of Jaccard is faster than the "non-check" version.

 * Arguments:
    * `list_expression1` : An `UnorderedList` or `OrderedList`.
    * `list_expression2` : An `UnorderedList` or `OrderedList`.
    * `threshold` : A `float` that represents the similarity threshold.
 * Return Value:
    * An `OrderedList` with two items:
     * The first item contains a `boolean` value representing whether `list_expression1` and `list_expression2` are similar.
     * The second item contains a `float` that represents the Jaccard similarity of `list_expression1` and `list_expression2` if it is greater than or equal to the threshold, or 0 otherwise.

 * Example:

        use dataverse TinySocial;

        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard-check($user.friend-ids, [1,5,9], 0.6f)
        where $sim[0]
        return $sim[1]


 * The expected result is:

        0.75f
        1.0f


### Similarity Operator ~# ###
 * "`~=`" is syntactic sugar for expressing a similarity condition with a given similarity threshold.
 * The similarity function and threshold for "`~=`" are controlled via "set" directives.
 * The "`~=`" operator returns a `boolean` value that represents whether the operands are similar.

 * Example for Jaccard similarity:

        use dataverse TinySocial;

        set simfunction "jaccard";
        set simthreshold "0.6f";

        for $user in dataset('FacebookUsers')
        where $user.friend-ids ~= [1,5,9]
        return $user


 * The expected result is:

        {
        "id": 3, "alias": "Emory", "name": "EmoryUnk", "user-since": datetime("2012-07-10T10:10:00.000Z"), "friend-ids": {{ 1, 5, 8, 9 }},
        "employment": [ { "organization-name": "geomedia", "start-date": date("2010-06-17"), "end-date": date("2010-01-26") } ]
        }
        {
        "id": 10, "alias": "Bram", "name": "BramHatch", "user-since": datetime("2010-10-16T10:10:00.000Z"), "friend-ids": {{ 1, 5, 9 }},
        "employment": [ { "organization-name": "physcane", "start-date": date("2007-06-05"), "end-date": date("2011-11-05") } ]
        }


 * Example for edit-distance similarity:

        use dataverse TinySocial;

        set simfunction "edit-distance";
        set simthreshold "2";

        for $user in dataset('FacebookUsers')
        where $user.name ~= "Suzanna Tilson"
        return $user


 * The expected output is:

        {
        "id": 7, "alias": "Suzanna", "name": "SuzannaTillson", "user-since": datetime("2012-08-07T10:10:00.000Z"), "friend-ids": {{ 6 }},
        "employment": [ { "organization-name": "Labzatron", "start-date": date("2011-04-19"), "end-date": null } ]
        }


## <a id="TokenizingFunctions">Tokenizing Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##
### word-tokens ###
 * Syntax:

        word-tokens(string_expression)

 * Returns a list of word tokens of `string_expression`.
 * Arguments:
    * `string_expression` : A `string` that will be tokenized.
 * Return Value:
    * An `OrderedList` of `string` word tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "word-tokens": [ "love", "verizon", "its", "voicemail", "service", "is", "awesome" ] }


<!--### hashed-word-tokens ###
 * Syntax:

        hashed-word-tokens(string_expression)

 * Returns a list of hashed word tokens of `string_expression`.
 * Arguments:
    * `string_expression` : A `string` that will be tokenized.
 * Return Value:
   * An `OrderedList` of `int32` hashed tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := hashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "hashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### counthashed-word-tokens ###
 * Syntax:

        counthashed-word-tokens(string_expression)

 * Returns a list of hashed word tokens of `string_expression`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
    * `string_expression` : A `String` that will be tokenized.
 * Return Value:
    * An `OrderedList` of `Int32` hashed tokens.
 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := counthashed-word-tokens($t.message-text)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-word-tokens": $tokens
        }


 * The expected result is:

        { "tweetid": "9", "counthashed-word-tokens": [ -1217719622, -447857469, -1884722688, -325178649, 210976949, 285049676, 1916743959 ] }


### gram-tokens ###
 * Syntax:

        gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of gram tokens of `string_expression`, which can be obtained by scanning the characters using a sliding window of a fixed length.
 * Arguments:
    * `string_expression` : A `String` that will be tokenized.
    * `gram_length` : An `Int32` as the length of grams.
   * `boolean_expression` : A `Boolean` value to indicate whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
    * An `OrderedList` of String gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "gram-tokens": [ "## ", "# l", " lo", "lov", "ove", "ve ", "e v", " ve", "ver", "eri", "riz", "izo", "zon", "on ", "n i", " it", "its", "ts ", "s v", " vo", "voi", "oic", "ice",
        "cem", "ema", "mai", "ail", "il-", "l-s", "-se", "ser", "erv", "rvi", "vic", "ice", "ce ", "e i", " is", "is ", "s a", " aw", "awe", "wes", "eso", "som", "ome", "me$", "e$$" ]
        }


### hashed-gram-tokens ###
 * Syntax:

        hashed-gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string_expression`.
 * Arguments:
    * `string_expression` : A `String` that will be tokenized.
    * `gram_length` : An `Int32` as the length of grams.
    * `boolean_expression` : A `Boolean` to indicate whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
    * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := hashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "hashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "hashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066,
        -937135958, -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734513, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131,
        1682609873, 2935161, -414769471, -1027490137, 1602276102, 1050490461 ]
        }


### counthashed-gram-tokens ###
 * Syntax:

        counthashed-gram-tokens(string_expression, gram_length, boolean_expression)

 * Returns a list of hashed gram tokens of `string_expression`. The hashing mechanism gives duplicate tokens different hash values, based on the occurrence count of that token.
 * Arguments:
    * `string_expression` : A `String` that will be tokenized.
    * `gram_length` : An `Int32`, length of grams to generate.
    * `boolean_expression` : A `Boolean`, whether to generate additional grams by pre- and postfixing `string_expression` with special characters.
 * Return Value:
    * An `OrderedList` of `Int32` hashed gram tokens.

 * Example:

        use dataverse TinySocial;

        for $t in dataset('TweetMessages')
        let $tokens := counthashed-gram-tokens($t.message-text, 3, true)
        where $t.send-time >= datetime('2012-01-01T00:00:00')
        return {
        "tweetid": $t.tweetid,
        "counthashed-gram-tokens": $tokens
        }


 * The expected result is:

        {
        "tweetid": "9",
        "counthashed-gram-tokens": [ 40557178, -2002241593, 161665899, -856104603, -500544946, 693410611, 395674299, -1015235909, 1115608337, 1187999872, -31006095, -219180466, -1676061637,
        1040194153, -1339307841, -1527110163, -1884722688, -179148713, -431014627, -1789789823, -1209719926, 684519765, -486734513, 1734740619, -1971673751, -932421915, -2064668066, -937135958,
        -790946468, -69070309, 1561601454, 26169001, -160734571, 1330043462, -486734512, -18796768, -470303314, 113421364, 1615760212, 1688217556, 1223719184, 536568131, 1682609873, 2935161,
        -414769471, -1027490137, 1602276102, 1050490461 ]
        }
-->

## <a id="TemporalFunctions">Temporal Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### date ###
 * Syntax:

        date(string_expression)

 * Constructor function for `date` type by parsing a date string `string_expression`.
 * Arguments:
    * `string_expression` : The `string` value representing a date value.
 * Return Value:
    * A `date` value represented by the given string.

 * Example:

        {
        "date-extended": date("2013-04-01"),
        "date-basic": date("20130401")
        }


 * The expected result is:

        {
        "date-extended": date("2013-04-01"),
        "date-basic": date("2013-04-01")
        }


### time ###
 * Syntax:

        time(string_expression)

 * Constructor function for `time` type by parsing a time string `string_expression`.
 * Arguments:
    * `string_expression` : The `string` value representing a time value.
 * Return Value:
    * A `time` value represented by the given string.

 * Example:

        {
        "time-extended": time("12:30:45.678+08:00"),
        "time-basic": time("123045678+0800")
        }


 * The expected result is:

        {
        "time-extended": time("04:30:45.678Z"),
        "time-basic": time("04:30:45.678Z")
        }


### datetime ###
 * Syntax:

        datetime(string_expression)

 * Constructor function for the `datetime` type by parsing a datetime string `string_expression`.
 * Arguments:
    * `string_expression` : The `string` value representing a datetime value.
 * Return Value:
    * A `datetime` value represented by the given string.

 * Example:

        {
        "datetime-extended": datetime("2013-04-01T12:30:45.678+08:00"),
        "datetime-basic": datetime("20130401T123045678+0800")
        }


 * The expected result is:

        {
        "datetime-extended": datetime("2013-04-01T04:30:45.678Z"),
        "datetime-basic": datetime("2013-04-01T04:30:45.678Z")
        }


### interval-from-date ###
 * Syntax:

        interval-from-date(string_expression1, string_expression2)

 * Constructor function for the `interval` type by parsing two date strings.
 * Arguments:
    * `string_expression1` : The `string` value representing the starting date.
    * `string_expression2` : The `string` value representing the ending date.
 * Return Value:
    * An `interval` value between the two dates.

 * Example:

        {"date-interval": interval-from-date("2012-01-01", "2013-04-01")}


 * The expected result is:

        { "date-interval": interval-date("2012-01-01, 2013-04-01") }


### interval-from-time ###
 * Syntax:

        interval-from-time(string_expression1, string_expression2)

 * Constructor function for the `interval` type by parsing two time strings.
 * Arguments:
    * `string_expression1` : The `string` value representing the starting time.
    * `string_expression2` : The `string` value representing the ending time.
 * Return Value:
    * An `interval` value between the two times.

 * Example:

        {"time-interval": interval-from-time("12:23:34.456Z", "233445567+0800")}


 * The expected result is:

        { "time-interval": interval-time("12:23:34.456Z, 15:34:45.567Z") }


### interval-from-datetime ###
 * Syntax:

        interval-from-datetime(string_expression1, string_expression2)

 * Constructor function for `interval` type by parsing two datetime strings.
 * Arguments:
    * `string_expression1` : The `string` value representing the starting datetime.
    * `string_expression2` : The `string` value representing the ending datetime.
 * Return Value:
    * An `interval` value between the two datetimes.

 * Example:

        {"datetime-interval": interval-from-datetime("2012-01-01T12:23:34.456+08:00", "20130401T153445567Z")}


 * The expected result is:

        { "datetime-interval": interval-datetime("2012-01-01T04:23:34.456Z, 2013-04-01T15:34:45.567Z") }


### year/month/day/hour/minute/second/millisecond ###
 * Syntax:

        year/month/day/hour/minute/second/millisecond(temporal_expression)

 * Accessors for accessing fields in a temporal value
 * Arguments:
    * `temporal_expression` : a temporal value represented as one of the following types: `date`, `datetime`, `time`, and `duration`.
 * Return Value:
    * An `int32` value representing the field to be extracted.

 * Example:

        let $c1 := date("2010-10-30")
        let $c2 := datetime("1987-11-19T23:49:23.938")
        let $c3 := time("12:23:34.930+07:00")
        let $c4 := duration("P3Y73M632DT49H743M3948.94S")

        return {"year": year($c1), "month": month($c2), "day": day($c1), "hour": hour($c3), "min": minute($c4), "second": second($c2), "ms": millisecond($c4)}


 * The expected result is:

        { "year": 2010, "month": 11, "day": 30, "hour": 5, "min": 28, "second": 23, "ms": 94 }


### adjust-datetime-for-timezone ###
 * Syntax:

        adjust-datetime-for-timezone(datetime_expression, string_expression)

 * Adjusts the given datetime `datetime_expression` by applying the timezone information `string_expression`.
 * Arguments:
    * `datetime_expression` : A `datetime` value to be adjusted.
    * `string_expression` : A `string` representing the timezone information.
 * Return Value:
    * A `string` value representing the new datetime after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        return {"adjusted-send-time": adjust-datetime-for-timezone($i.send-time, "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "2008-04-26T18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "2010-05-13T18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2006-11-04T18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "2011-12-26T18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "2006-08-04T18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "2010-05-07T18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "2011-08-25T18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "2005-10-14T18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "2012-07-21T18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "2008-01-26T18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "2008-03-09T18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "2010-02-13T18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### adjust-time-for-timezone ###
 * Syntax:

        adjust-time-for-timezone(time_expression, string_expression)

 * Adjusts the given time `time_expression` by applying the timezone information `string_expression`.
 * Arguments:
    * `time_expression` : A `time` value to be adjusted.
    * `string_expression` : A `string` representing the timezone information.
 * Return Value:
    * A `string` value representing the new time after being adjusted by the timezone information.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        return {"adjusted-send-time": adjust-time-for-timezone(time-from-datetime($i.send-time), "+08:00"), "message": $i.message-text}


 * The expected result is:

        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love t-mobile its customization is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like verizon its shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like motorola the speed is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like sprint the voice-command is mind-blowing:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand motorola its speed is terrible:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like iphone the voice-clarity is good:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the platform is good" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like t-mobile the shortcut-menu is awesome:)" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " love verizon its voicemail-service is awesome" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " hate verizon its voice-clarity is OMG:(" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " can't stand iphone its platform is terrible" }
        { "adjusted-send-time": "18:10:00.000+08:00", "message": " like samsung the voice-command is amazing:)" }


### calendar-duration-from-datetime ###
 * Syntax:

        calendar-duration-from-datetime(datetime_expression, duration_expression)

 * Gets a user-friendly representation of the duration `duration_expression` based on the given datetime `datetime_expression`.
 * Arguments:
    * `datetime_expression` : A `datetime` value to be used as the reference time point.
    * `duration_expression` : A `duration` value to be converted.
 * Return Value:
    * A `duration` value with the duration as `duration_expression` but with a user-friendly representation.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"since-2011": subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")), "since-2011-user-friendly": calendar-duration-from-datetime($i.send-time, subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")))}


 * The expected result is:

        { "since-2011": duration("P359DT10H10M"), "since-2011-user-friendly": duration("P11M23DT10H10M") }
        { "since-2011": duration("P236DT10H10M"), "since-2011-user-friendly": duration("P7M23DT10H10M") }
        { "since-2011": duration("P567DT10H10M"), "since-2011-user-friendly": duration("P1Y6M18DT10H10M") }


### calendar-duration-from-date ###
 * Syntax:

        calendar-duration-from-date(date_expression, duration_expression)

 * Gets a user-friendly representation of the duration `duration_expression` based on the given date `date_expression`.
 * Arguments:
    * `date_expression` : A `date` value to be used as the reference time point.
    * `duration_expression` : A `duration` value to be converted.
 * Return Value:
    * A `duration` value with the duration as `duration_expression` but with a user-friendly representation.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"since-2011": subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")),
        "since-2011-user-friendly": calendar-duration-from-date(date-from-datetime($i.send-time), subtract-datetime($i.send-time, datetime("2011-01-01T00:00:00")))}


 * The expected result is:

        { "since-2011": duration("P359DT10H10M"), "since-2011-user-friendly": duration("P11M23DT10H10M") }
        { "since-2011": duration("P236DT10H10M"), "since-2011-user-friendly": duration("P7M23DT10H10M") }
        { "since-2011": duration("P567DT10H10M"), "since-2011-user-friendly": duration("P1Y6M18DT10H10M") }


### current-date ###
 * Syntax:

        current-date()

 * Gets the current date.
 * Arguments: None
 * Return Value:
    * A `date` value of the date when the function is called.

### current-time ###
 * Syntax:

        current-time()

 * Get the current time
 * Arguments: None
 * Return Value:
    * A `time` value of the time when the function is called.

### current-datetime ###
 * Syntax:

        current-datetime()

 * Get the current datetime
 * Arguments: None
 * Return Value:
    * A `datetime` value of the datetime when the function is called.

 * Example:

        use dataverse TinySocial;

        {"current-date": current-date(),
        "current-time": current-time(),
        "current-datetime": current-datetime()}


 * The expected result is:

        { "current-date": date("2013-04-06"),
        "current-time": time("00:48:44.093Z"),
        "current-datetime": datetime("2013-04-06T00:48:44.093Z") }


### date-from-datetime ###
 * Syntax:

        date-from-datetime(datetime_expression)

 * Gets the date value from the given datetime value `datetime_expression`.
 * Arguments:
    * `datetime_expression`: A `datetime` value to be extracted from.
 * Return Value:
    * A `date` value from the datetime.

### time-from-datetime ###
 * Syntax:

        time-from-datetime(datetime_expression)

 * Get the time value from the given datetime value `datetime_expression`
 * Arguments:
    * `datetime_expression`: A `datetime` value to be extracted from
 * Return Value:
    * A `time` value from the datetime.

 * Example:

        use dataverse TinySocial;

        for $i in dataset('TweetMessages')
        where $i.send-time > datetime("2011-01-01T00:00:00")
        return {"send-date": date-from-datetime($i.send-time), "send-time": time-from-datetime($i.send-time)}


 * The expected result is:

        { "send-date": date("2011-12-26"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2011-08-25"), "send-time": time("10:10:00.000Z") }
        { "send-date": date("2012-07-21"), "send-time": time("10:10:00.000Z") }


### date-from-unix-time-in-days ###
 * Syntax:

        date-from-unix-time-in-days(numeric_expression)

 * Gets a date representing the time after `numeric_expression` days since 1970-01-01.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32` value representing the number of days.
 * Return Value:
    * A `date` value as the time after `numeric_expression` days since 1970-01-01.

### datetime-from-unix-time-in-ms ###
 * Syntax:

        datetime-from-unix-time-in-ms(numeric_expression)

 * Gets a datetime representing the time after `numeric_expression` milliseconds since 1970-01-01T00:00:00Z.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32`/`int64` value representing the number of milliseconds.
 * Return Value:
    * A `datetime` value as the time after `numeric_expression` milliseconds since 1970-01-01T00:00:00Z.

### time-from-unix-time-in-ms ###
 * Syntax:

        time-from-unix-time-in-ms(numeric_expression)

 * Gets a time representing the time after `numeric_expression` milliseconds since 00:00:00.000Z.
 * Arguments:
    * `numeric_expression`: A `int8`/`int16`/`int32` value representing the number of milliseconds.
 * Return Value:
    * A `time` value as the time after `numeric_expression` milliseconds since 00:00:00.000Z.

 * Example:

        use dataverse TinySocial;

        let $d := date-from-unix-time-in-days(15800)
        let $dt := datetime-from-unix-time-in-ms(1365139700000)
        let $t := time-from-unix-time-in-ms(3748)
        return {"date": $d, "datetime": $dt, "time": $t}


 * The expected result is:

        { "date": date("2013-04-05"), "datetime": datetime("2013-04-05T05:28:20.000Z"), "time": time("00:00:03.748Z") }

### subtract-date ###
 * Syntax:

        subtract-date(date_start, date_end)

 * Get the duration between two dates `date_start` and `date_end`
 * Arguments:
    * `date_start`: the starting `date`
    * `date_end`: the ending `date`
 * Return Value:
    * A `duration` value between `date_start` and `date_end`

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2012-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-date(date-from-datetime($j.user-since), date-from-datetime($i.user-since))}


 * The expected result is:

        { "id1": 3, "id2": 1, "diff": duration("P41D") }
        { "id1": 3, "id2": 7, "diff": duration("P28D") }
        { "id1": 7, "id2": 1, "diff": duration("P13D") }


### subtract-time ###
 * Syntax:

        subtract-time(time_start, time_end)

 * Get the duration between two times `time_start` and `time_end`
 * Arguments:
    * `time_start`: the starting `time`
    * `time_end`: the ending `time`
 * Return Value:
    * A `duration` value between `time_start` and `time_end`

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2012-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-time(time-from-datetime($j.user-since), time("02:50:48.938"))}


 * The expected result is:

        { "id1": 3, "id2": 1, "diff": duration("PT7H19M11.62S") }
        { "id1": 3, "id2": 7, "diff": duration("PT7H19M11.62S") }
        { "id1": 7, "id2": 1, "diff": duration("PT7H19M11.62S") }


### subtract-datetime ###
 * Syntax:

        subtract-datetime(datetime_start, datetime_end)

 * Get the duration between two datetimes `datetime_start` and `datetime_end`
 * Arguments:
    * `datetime_start`: the starting `datetime`
    * `datetime_end`: the ending `datetime`
 * Return Value:
    * A `duration` value between `datetime_start` and `datetime_end`

 * Example:

        use dataverse TinySocial;

        for $i in dataset('FacebookUser')
        for $j in dataset('FacebookUser')
        where $i.user-since < $j.user-since and $i.user-since > datetime("2011-01-01T00:00:00")
        return {"id1": $i.id, "id2": $j.id, "diff": subtract-datetime($j.user-since, $i.user-since)}


 * The expected result is:

        { "id1": 2, "id2": 1, "diff": duration("P576D") }
        { "id1": 2, "id2": 3, "diff": duration("P535D") }
        { "id1": 2, "id2": 7, "diff": duration("P563D") }
        { "id1": 3, "id2": 1, "diff": duration("P41D") }
        { "id1": 3, "id2": 7, "diff": duration("P28D") }
        { "id1": 7, "id2": 1, "diff": duration("P13D") }

### interval-start-from-date/time/datetime ###
 * Syntax:

        interval-start-from-date/time/datetime(date/time/datetime, duration)

 * Construct an `interval` value by the given starting `date`/`time`/`datetime` and the `duration` that the interval lasts.
 * Arguments:
    * `date/time/datetime`: a `string` representing a `date`, `time` or `datetime`, or a `date`/`time`/`datetime` value, representing the starting time point.
    * `duration`: a `string` or `duration` value representing the duration of the interval. Note that duration cannot be negative value.
 * Return Value:
    * An `interval` value representing the interval starting from the given time point with the length of duration.

 * Example:

        let $itv1 := interval-start-from-date("1984-01-01", "P1Y")
        let $itv2 := interval-start-from-time(time("02:23:28.394"), "PT3H24M")
        let $itv3 := interval-start-from-datetime("1999-09-09T09:09:09.999", duration("P2M30D"))
        return {"interval1": $itv1, "interval2": $itv2, "interval3": $itv3}

 * The expectecd result is:

        { "interval1": interval-date("1984-01-01, 1985-01-01"), "interval2": interval-time("02:23:28.394Z, 05:47:28.394Z"), "interval3": interval-datetime("1999-09-09T09:09:09.999Z, 1999-12-09T09:09:09.999Z") }

### get-interval-start, get-interval-end ###
 * Syntax:

        get-interval-start/get-interval-end(interval)

 * Gets the start/end of the given interval.
 * Arguments:
    * `interval`: the interval to be accessed.
 * Return Value:
    * A `time`, `date`, or `datetime` (depending on the time instances of the interval) representing the starting or ending time.

 * Example:

        let $itv := interval-start-from-date("1984-01-01", "P1Y")
        return {"start": get-interval-start($itv), "end": get-interval-end($itv)}


 * The expected result is:

        { "start": date("1984-01-01"), "end": date("1985-01-01") }

### interval-bin ###
 * Syntax:

        interval-bin(time-to-bin, time-bin-anchor, duration-bin-size)

 * Return the `interval` value representing the bin containing the `time-to-bin` value.
 * Arguments:
    * `time-to-bin`: a date/time/datetime value representing the time to be binned.
    * `time-bin-anchor`: a date/time/datetime value representing an anchor of a bin starts. The type of this argument should be the same as the first `time-to-bin` argument.
    * `duration-bin-size`: the duration value representing the size of the bin, in the type of year-month-duration or day-time-duration. The type of this duration should be compatible with the type of `time-to-bin`, so that the arithmetic operation between `time-to-bin` and `duration-bin-size` is well-defined. Currently AsterixDB supports the following arithmetic operations:
        * datetime +|- year-month-duration
        * datetime +|- day-time-duration
        * date +|- year-month-duration
        * date +|- day-time-duration
        * time +|- day-time-duration
  * Return Value:
    * A `interval` value representing the bin containing the `time-to-bin` value. Note that the internal type of this interval value should be the same as the `time-to-bin` type.

  * Example:

        let $c1 := date("2010-10-30")
        let $c2 := datetime("-1987-11-19T23:49:23.938")
        let $c3 := time("12:23:34.930+07:00")

        return { "bin1": interval-bin($c1, date("1990-01-01"), year-month-duration("P1Y")),
         "bin2": interval-bin($c2, datetime("1990-01-01T00:00:00.000Z"), year-month-duration("P6M")),
         "bin3": interval-bin($c3, time("00:00:00"), day-time-duration("PD1M")),
         "bin4": interval-bin($c2, datetime("2013-01-01T00:00:00.000"), day-time-duration("PT24H"))
       }

   * The expected result is:

        { "bin1": interval-date("2010-01-01, 2011-01-01"),
          "bin2": interval-datetime("-1987-07-01T00:00:00.000Z, -1986-01-01T00:00:00.000Z"),
          "bin3": interval-time("05:23:00.000Z, 05:24:00.000Z"),
          "bin4": interval-datetime("-1987-11-19T00:00:00.000Z, -1987-11-20T00:00:00.000Z")}

## <a id="OtherFunctions">Other Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

### is-null ###
 * Syntax:

        is-null(var)

 * Checks whether the given variable is a `null` value.
 * Arguments:
    * `var` : A variable (any type is allowed).
 * Return Value:
    * A `boolean` on whether the variable is a `null` or not.

 * Example:

        for $m in ['hello', 'world', null]
        where not(is-null($m))
        return $m


 * The expected result is:

        "hello"
        "world"

### switch-case ###
 * Syntax:

        switch-case(condition,
            case1, case1-result,
            case2, case2-result,
            ...,
            default, default-result
        )

 * Switches amongst a sequence of cases and returns the result of the first matching case. If no match is found, the result of the default case is returned.
 * Arguments:
    * `condition`: A variable (any type is allowed).
    * `caseI/default`: A variable (any type is allowed).
    * `caseI/default-result`: A variable (any type is allowed).
 * Return Value:
    * Returns `caseI-result` if `condition` matches `caseI`, otherwise `default-result`.
 * Example 1:

        switch-case("a",
            "a", 0,
            "x", 1,
            "y", 2,
            "z", 3
        )


 * The expected result is:

        0


 * Example 2:

        switch-case("a",
            "x", 1,
            "y", 2,
            "z", 3
        )


 * The expected result is:

        3
