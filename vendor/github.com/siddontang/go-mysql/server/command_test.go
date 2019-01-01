package server

// Ensure EmptyHandler implements Handler interface or cause compile time error
var _ Handler = EmptyHandler{}
