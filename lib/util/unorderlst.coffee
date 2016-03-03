# operations for unordered list
exports.rm = (lst, pos) ->
  len = lst.length
  return unless len

  idx = lst.indexOf pos
  return unless !!~idx

  last = lst.pop()
  lst[idx] = last unless idx is len - 1
