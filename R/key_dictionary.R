#' Replaces or appends the surrogate key from the key dictionary with the
#' original key where the key category and surrogate keys match.
#'
#'#' @details
#' There are a number of checks performed to identify problems. Each check is
#' identified by a code with a description.
#'
#' @param data_frame The data frame in which the key column exists. The data
#'  frame can be an R object or a file.
#' @param column The key column that will be matched from the data frame to the
#'  key dictionary.
#' @param original_key_column_name The name of the column in which the original
#'   key should be populated from the key dictionary.
#' @param ... Reserved for future use.
#' @param key_dictionary The key dictionary object or a connection (e.g. file
#'   location) to the key dictionary. Defaults to the file `key_dictionary.rds`
#'   in the working directory. Contains the original key, surrogate key, key
#'   category and the date and time a new row was created.
#' @param key_category The name of the key category. Defaults to the value of
#'   the column.
#' @param keep_surrogate_key_column Retain the surrogate key column if set to
#'   `TRUE`. Otherwise, remove the surrogate key column. Defaults to `FALSE`.
#' @return Return the processed data frame.
#' @export
#'
attach_original_key <-
  function(data_frame,
           column,
           original_key_column_name,
           ...,
           key_dictionary = "key_dictionary.rds",
           key_category,
           keep_surrogate_key_column = FALSE) {

    if (!(is.data.frame(data_frame))) {
      stop("The object provided is not a data frame. Please pass in a valid ",
           "data frame object.")
    }

    if (nrow(data_frame) == 0) {
      stop("The data frame passed into the `attach_original_key` function ",
           "via the `data_frame` parameter has no rows.")
    }

    if (!(column %in% colnames(data_frame))) {
      stop("The column in which the original key will be added, as specified ",
           "by the `column` parameter is NOT present in the provided data ",
           "frame. Please pass in a data frame with valid column names.")
    }

    if (is.data.frame(key_dictionary)) {
      key_dictionary_df <- key_dictionary
    } else if (is.character(key_dictionary) && file.exists(key_dictionary)) {
      key_dictionary_df <- readRDS(key_dictionary)
    } else {
      stop("The key_dictionary parameter must be a data frame or a file path.",
           "The key_dictionary parameter has a class of ",
           class(key_dictionary), ".")
    }

    # Handle global visible binding issue
    source_key_idc <- surrogate_key_idc <- NULL

    key_categories <- unique(key_dictionary_df$key_category)
    if (!(key_category %in% key_categories)) {
      stop("The specified key category (", key_category, ") does not exist in ",
           "the key dictionary. Key categories in the key dictionary are ",
           paste(
             key_categories[order(tolower(key_categories))],
             collapse = ", ",
             sep = ""
           ), ".")
    }

    # Filter the key dictionary using the appropriate category in the
    # key_category column. Here the bang bang (!!) technique is used to unquote
    # the key_category argument of the function. The source_key_idc and
    # surrogate_key_idc columns are then selected for the key_dictionary output.

    filtered_key_dictionary <-
      key_dictionary_df %>%
      dplyr::filter(key_category == !!key_category) %>%
      dplyr::select(source_key_idc, surrogate_key_idc)

    # The filtered_key_dictionary is joined to the data_frame matching on the
    # surrogate_key_idc and the name used for the source_key_idc in the
    # data_frame. The original key column in the data_frame is renamed
    # 'source_key_idc' and then this column is moved to the first position. If
    # the surrogate key is being retained this column is placed after the
    # source_key_idc column.

    processed_data_frame <-
      data_frame %>%
      dplyr::left_join(filtered_key_dictionary, by = stats::setNames(
        "surrogate_key_idc", column)) %>%
      dplyr::rename(!!original_key_column_name := "source_key_idc") %>%
      dplyr::select(!!original_key_column_name, tidyselect::everything()) %>%
      dplyr::select(tidyselect::all_of(column), !!original_key_column_name,
                    tidyselect::everything())

    if (keep_surrogate_key_column == FALSE) {
      return(dplyr::select(processed_data_frame, -tidyselect::all_of(column)))
    }

    return(processed_data_frame)
  }

#' Replaces or appends the original key with a surrogate key associated with a
#' key category and stored in a key dictionary.
#'
#' @param data_frame The data frame in which the key column exists. The data
#'  frame can be an R object or a file.
#' @param column The key column that will be matched from the data frame to the
#'  key dictionary.
#' @param surrogate_key_column_name The name of the column in which the
#'   surrogate key should be populated from the key dictionary.
#' @param ... Reserved for future use.
#' @param key_dictionary The key dictionary object or a connection (e.g. file
#'   location) to the key dictionary. Defaults to the file `key_dictionary.rds`
#'   in the working directory. Contains the original key, surrogate key, key
#'   category and the date and time a new row was created.
#' @param key_category The name of the key category. Defaults to the value of
#'   the column.
#' @param keep_original_key_column Retain the original key column if
#'   set to `TRUE`. Otherwise, remove the original key column. Defaults to
#'   `FALSE`.
#' @return Return the processed data frame.
#' @export
#'
#'

attach_surrogate_key <-
  function(data_frame,
           column,
           surrogate_key_column_name,
           ...,
           key_dictionary = "key_dictionary.rds",
           key_category,
           keep_original_key_column = FALSE) {

    if (!(is.data.frame(data_frame))) {
      stop("The object provided is not a data frame. Please pass in a valid ",
           "data frame object.")
    }

    if (nrow(data_frame) == 0) {
      stop("The data frame passed into the `attach_surrogate_key` function ",
           "via the `data_frame` parameter has no rows.")
    }

    if (!(column %in% colnames(data_frame))) {
      stop("The column in which a surrogate will be generated, as specified ",
           "by the `column` parameter is NOT present in the provided data ",
           "frame. Please pass in a data frame with valid column names.")
    }

    # Handle global visible binding issue
    source_key_idc <- surrogate_key_idc <- created_dttm <- NULL

    if (is.data.frame(key_dictionary)) {
      key_dictionary_df <- key_dictionary
      key_dictionary_path <- NULL
    } else if (is.character(key_dictionary) && file.exists(key_dictionary)) {
      key_dictionary_df <- readRDS(key_dictionary)
      key_dictionary_path <- key_dictionary
    } else {
      key_dictionary_df <- tibble::tibble(
        key_category = character(),
        source_key_idc = character(),
        surrogate_key_idc = character(),
        created_dttm = as.POSIXct(character(0))
      )
      key_dictionary_path <- ifelse(is.character(key_dictionary),
                                    key_dictionary,
                                    NULL)
    }

    new_dictionary_keys <-
      data_frame %>%
      dplyr::select(source_key_idc = !!column) %>%
      dplyr::filter(!is.na(source_key_idc)) %>%
      dplyr::distinct(source_key_idc) %>%
      dplyr::left_join(
        dplyr::filter(key_dictionary_df, key_category == !!key_category),
        by = c("source_key_idc")
      ) %>%
      dplyr::select(key_category, source_key_idc, surrogate_key_idc,
                    created_dttm) %>%
      dplyr::filter(is.na(surrogate_key_idc)) %>%
      dplyr::mutate(
        key_category = !!key_category,
        surrogate_key_idc =
          matrix(
            as.character(as.raw(sample(256L, 16 * nrow(.), TRUE) - 1L)),
            nrow(.)
          ) %>%
          apply(1, paste, collapse = ""),
        created_dttm = Sys.time()
      )

    key_dictionary_input <-
      dplyr::bind_rows(key_dictionary_df, new_dictionary_keys)

    if (!is.null(key_dictionary_path)) {
      saveRDS(key_dictionary_input, key_dictionary_path)
    }

    filtered_key_dictionary <-
      key_dictionary_input %>%
      dplyr::filter(key_category == !!key_category) %>%
      dplyr::select(source_key_idc, surrogate_key_idc)

    processed_data_frame <-
      data_frame %>%
      dplyr::left_join(filtered_key_dictionary,
                       by = stats::setNames("source_key_idc", column)) %>%
      dplyr::rename(!!surrogate_key_column_name := "surrogate_key_idc") %>%
      dplyr::select(!!surrogate_key_column_name, tidyselect::everything()) %>%
      dplyr::select(tidyselect::all_of(column), !!surrogate_key_column_name,
                    tidyselect::everything())

    if (keep_original_key_column == FALSE) {
      return(dplyr::select(processed_data_frame, -tidyselect::all_of(column)))
    }
    return(processed_data_frame)
}
