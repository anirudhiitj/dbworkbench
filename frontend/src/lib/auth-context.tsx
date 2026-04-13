import { createContext, useContext, useEffect, useState, useCallback, type ReactNode } from "react";
import { login as apiLogin, register as apiRegister, logout as apiLogout, refreshToken } from "@/lib/api";

interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  username: string | null;
}

interface AuthContextType extends AuthState {
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string, email: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

function parseJwt(token: string): Record<string, unknown> | null {
  try {
    const payload = token.split(".")[1];
    return JSON.parse(atob(payload));
  } catch {
    return null;
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: true,
    username: null,
  });

  const extractUsername = useCallback((token: string) => {
    const payload = parseJwt(token);
    return (payload?.username as string) || (payload?.user_id as string)?.toString() || "User";
  }, []);

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      const payload = parseJwt(token);
      const exp = payload?.exp as number;
      if (exp && exp * 1000 > Date.now()) {
        setState({ isAuthenticated: true, isLoading: false, username: extractUsername(token) });
      } else {
        refreshToken()
          .then((data) => {
            setState({ isAuthenticated: true, isLoading: false, username: extractUsername(data.access) });
          })
          .catch(() => {
            apiLogout();
            setState({ isAuthenticated: false, isLoading: false, username: null });
          });
        return;
      }
    }
    setState((s) => ({ ...s, isLoading: false }));
  }, [extractUsername]);

  const login = async (username: string, password: string) => {
    const data = await apiLogin(username, password);
    setState({ isAuthenticated: true, isLoading: false, username: extractUsername(data.access) });
  };

  const register = async (username: string, password: string, email: string) => {
    await apiRegister(username, password, email);
    await login(username, password);
  };

  const logout = () => {
    apiLogout();
    setState({ isAuthenticated: false, isLoading: false, username: null });
  };

  return (
    <AuthContext.Provider value={{ ...state, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
